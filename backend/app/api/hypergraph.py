"""
API routes for hypergraph extraction and visualization
"""
from fastapi import APIRouter, HTTPException, Depends, Query
import logging
import json

from app.models.hypergraph import Hypergraph
from app.models.query import QueryMetadata
from app.spark.spark_manager import SparkManager
from app.hypergraph.extractor import HypergraphExtractor
from app.hypergraph.plan_to_json import get_plan_as_json
from app.dependencies import get_spark_manager
from app.api.queries import get_query

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/extract", response_model=Hypergraph)
async def extract_hypergraph(
    sql: str,
    query_id: str = None,
    include_schema_attributes: bool = False,
    spark_manager: SparkManager = Depends(get_spark_manager)
):
    """
    Extract hypergraph structure from a SQL query

    - **sql**: SQL query string
    - **query_id**: Optional query identifier
    - **include_schema_attributes**: Whether to include all schema attributes as nodes (default: False)

    Returns the hypergraph representation including:
    - Nodes (relations/tables)
    - Hyperedges (joins)
    - Acyclicity information
    - Hypertree decomposition (if available)
    """
    try:
        extractor = HypergraphExtractor(spark_manager.spark)
        hypergraph = extractor.extract_hypergraph(sql, query_id, include_schema_attributes)

        logger.info(
            f"Extracted hypergraph: {hypergraph.num_relations} relations, "
            f"{hypergraph.num_joins} joins, acyclic: {hypergraph.is_acyclic}"
        )

        return hypergraph

    except Exception as e:
        logger.error(f"Hypergraph extraction failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=400,
            detail=f"Failed to extract hypergraph: {str(e)}"
        )


@router.get("/{query_id}", response_model=Hypergraph)
async def get_query_hypergraph(
    query_id: str,
    include_schema_attributes: bool = False,
    spark_manager: SparkManager = Depends(get_spark_manager)
):
    """
    Get hypergraph for a query from the catalog

    - **query_id**: Query identifier from the catalog
    - **include_schema_attributes**: Whether to include all schema attributes as nodes (default: False)
    """
    # Get the query metadata
    query = await get_query(query_id)

    # Extract hypergraph
    extractor = HypergraphExtractor(spark_manager.spark)
    hypergraph = extractor.extract_hypergraph(query.sql, query_id, include_schema_attributes)

    return hypergraph


@router.post("/visualize")
async def generate_visualization_data(hypergraph: Hypergraph):
    """
    Generate Cytoscape.js compatible visualization data from hypergraph

    Converts the hypergraph model into a format suitable for frontend visualization
    """
    try:
        # Log incoming hypergraph structure
        logger.info(f"Visualize endpoint received hypergraph with {len(hypergraph.edges)} edges and {len(hypergraph.nodes)} nodes")
        for edge in hypergraph.edges:
            logger.info(f"  Input edge {edge.id}: {len(edge.attributes)} attributes, {len(edge.output_attributes)} output attributes")

        # Convert to Cytoscape.js format
        elements = []

        # Add nodes
        for node in hypergraph.nodes:
            elements.append({
                "data": {
                    "id": node.id,
                    "label": node.label,
                    "type": node.type,
                    "attributes": ", ".join(node.attributes),
                    "output_attributes": ", ".join(node.output_attributes),
                    "cardinality": node.cardinality
                },
                "classes": node.type
            })

        # Add hyperedges
        # For hypergraphs in Cytoscape, we need to create compound nodes or
        # use multiple edges to represent hyperedges
        logger.info(f"Converting {len(hypergraph.edges)} hyperedges to visualization format")

        for edge in hypergraph.edges:
            logger.info(f"Processing edge {edge.id} with {len(edge.nodes)} connected nodes: {edge.nodes}")

            if len(edge.nodes) == 0:
                # Isolated hyperedge (table with no join attributes) - create standalone node
                logger.info(f"Creating isolated hyperedge node for {edge.id} (no joins)")
                hyperedge_node_id = f"{edge.id}_node"
                elements.append({
                    "data": {
                        "id": hyperedge_node_id,
                        "label": edge.label or edge.id,
                        "type": "hyperedge",
                        "attributes": ", ".join(edge.attributes) if edge.attributes else "",
                        "output_attributes": ", ".join(edge.output_attributes) if edge.output_attributes else ""
                    },
                    "classes": "hyperedge isolated"
                })
            elif len(edge.nodes) == 1:
                # Single node - create a self-loop or isolated node marker
                logger.info(f"Edge {edge.id} connects to single node {edge.nodes[0]}")
                # For visualization, create a hyperedge node
                hyperedge_node_id = f"{edge.id}_node"
                elements.append({
                    "data": {
                        "id": hyperedge_node_id,
                        "label": edge.label or edge.id,
                        "type": "hyperedge",
                        "attributes": ", ".join(edge.attributes) if edge.attributes else "",
                        "output_attributes": ", ".join(edge.output_attributes) if edge.output_attributes else ""
                    },
                    "classes": "hyperedge"
                })
                elements.append({
                    "data": {
                        "id": f"{edge.id}_conn_0",
                        "source": hyperedge_node_id,
                        "target": edge.nodes[0]
                    },
                    "classes": "hyperedge-connection"
                })
            elif len(edge.nodes) == 2:
                # 2-vertex hyperedge - create hyperedge node + connections
                attrs_str = ", ".join(edge.attributes) if edge.attributes else ""
                output_attrs_str = ", ".join(edge.output_attributes) if edge.output_attributes else ""
                logger.info(f"Creating hyperedge node for {edge.id} between {edge.nodes[0]} and {edge.nodes[1]}")
                logger.info(f"  Edge {edge.id} attributes ({len(edge.attributes)}): {attrs_str[:100]}{'...' if len(attrs_str) > 100 else ''}")
                logger.info(f"  Edge {edge.id} output_attributes ({len(edge.output_attributes)}): {output_attrs_str}")

                # Create hyperedge node
                hyperedge_node_id = f"{edge.id}_node"
                elements.append({
                    "data": {
                        "id": hyperedge_node_id,
                        "label": edge.label or edge.id,
                        "type": "hyperedge",
                        "attributes": attrs_str,
                        "output_attributes": output_attrs_str
                    },
                    "classes": "hyperedge"
                })

                # Connect to both attribute nodes
                elements.append({
                    "data": {
                        "id": f"{edge.id}_conn_0",
                        "source": hyperedge_node_id,
                        "target": edge.nodes[0]
                    },
                    "classes": "hyperedge-connection"
                })
                elements.append({
                    "data": {
                        "id": f"{edge.id}_conn_1",
                        "source": hyperedge_node_id,
                        "target": edge.nodes[1]
                    },
                    "classes": "hyperedge-connection"
                })
            else:
                # Hyperedge with 3+ nodes - create a compound node
                logger.info(f"Creating hyperedge node for {edge.id} connecting {len(edge.nodes)} nodes")
                hyperedge_node_id = f"{edge.id}_node"
                elements.append({
                    "data": {
                        "id": hyperedge_node_id,
                        "label": edge.label or edge.id,
                        "type": "hyperedge",
                        "attributes": ", ".join(edge.attributes) if edge.attributes else "",
                        "output_attributes": ", ".join(edge.output_attributes) if edge.output_attributes else ""
                    },
                    "classes": "hyperedge"
                })

                # Connect all nodes to the hyperedge node
                for i, node_id in enumerate(edge.nodes):
                    elements.append({
                        "data": {
                            "id": f"{edge.id}_conn_{i}",
                            "source": hyperedge_node_id,
                            "target": node_id
                        },
                        "classes": "hyperedge-connection"
                    })

        # Generate layout hints
        layout = {
            "name": "cose",  # Force-directed layout works well for hypergraphs
            "animate": True,
            "nodeDimensionsIncludeLabels": True
        }

        result = {
            "elements": elements,
            "layout": layout,
            "stats": {
                "num_nodes": len(hypergraph.nodes),
                "num_edges": len(hypergraph.edges),
                "is_acyclic": hypergraph.is_acyclic,
                "hypertree_width": hypergraph.hypertree_width,
                "num_relations": hypergraph.num_relations,
                "num_joins": hypergraph.num_joins,
                "num_aggregates": hypergraph.num_aggregates
            }
        }

        logger.info(f"Generated visualization with {len(elements)} elements")
        logger.info(f"Stats: {result['stats']}")

        return result

    except Exception as e:
        logger.error(f"Visualization generation failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate visualization: {str(e)}"
        )


@router.get("/properties/{query_id}")
async def analyze_hypergraph_properties(
    query_id: str,
    spark_manager: SparkManager = Depends(get_spark_manager)
):
    """
    Analyze hypergraph properties for a query

    Returns detailed analysis including:
    - Acyclicity
    - Guardedness
    - Hypertree width
    - Optimization opportunities
    """
    query = await get_query(query_id)
    extractor = HypergraphExtractor(spark_manager.spark)
    hypergraph = extractor.extract_hypergraph(query.sql, query_id)

    # Analyze properties
    analysis = {
        "query_id": query_id,
        "is_acyclic": hypergraph.is_acyclic,
        "is_guarded": hypergraph.is_guarded,
        "hypertree_width": hypergraph.hypertree_width,
        "num_relations": hypergraph.num_relations,
        "num_joins": hypergraph.num_joins,
        "num_aggregates": hypergraph.num_aggregates,

        # Optimization recommendations
        "can_avoid_materialization": hypergraph.is_acyclic and hypergraph.num_aggregates > 0,
        "recommended_join_order": None,  # Would be computed from hypertree decomposition

        "explanation": _generate_explanation(hypergraph)
    }

    return analysis


def _generate_explanation(hypergraph: Hypergraph) -> str:
    """Generate human-readable explanation of hypergraph properties"""
    explanation = []

    if hypergraph.is_acyclic:
        explanation.append(
            "This query has an acyclic join structure, which enables efficient "
            "evaluation using Yannakakis' algorithm."
        )

    if hypergraph.num_aggregates > 0:
        explanation.append(
            f"The query contains {hypergraph.num_aggregates} aggregate operation(s). "
        )

        if hypergraph.is_acyclic:
            explanation.append(
                "With the acyclic structure, we can avoid materializing intermediate "
                "join results by pushing aggregates down into the join tree."
            )

    if not hypergraph.is_acyclic:
        explanation.append(
            "This query has a cyclic join structure, which limits some optimization opportunities."
        )

    return " ".join(explanation)


@router.post("/query-plan-json")
async def get_query_plan_json(
    sql: str,
    plan_type: str = Query(default="analyzed", regex="^(logical|analyzed|optimized)$"),
    spark_manager: SparkManager = Depends(get_spark_manager)
):
    """
    Get the Spark logical plan as JSON for a SQL query

    This endpoint exposes Spark's internal query plan representation as JSON,
    which can be used for debugging, analysis, or custom hypergraph extraction.

    Args:
        sql: SQL query string
        plan_type: Type of plan to extract (logical, analyzed, or optimized)
        spark_manager: Spark session manager

    Returns:
        JSON representation of the query plan

    Example:
        POST /api/hypergraph/query-plan-json
        {
            "sql": "SELECT * FROM table1 JOIN table2 ON table1.id = table2.id",
            "plan_type": "analyzed"
        }
    """
    try:
        if spark_manager.spark is None:
            raise HTTPException(status_code=503, detail="Spark session not initialized")

        # Parse the query
        df = spark_manager.spark.sql(sql)

        # Get plan as JSON
        plan_json = get_plan_as_json(df, plan_type)

        return {
            "sql": sql,
            "plan_type": plan_type,
            "plan": plan_json
        }

    except Exception as e:
        logger.error(f"Failed to extract query plan: {e}", exc_info=True)
        raise HTTPException(
            status_code=400,
            detail=f"Failed to extract query plan: {str(e)}"
        )
