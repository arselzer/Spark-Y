"""
Hypergraph extraction from SQL queries
Uses Spark logical plan JSON representation for accurate extraction
"""
import logging
import json
import re
from typing import Optional, Dict, Any, List, Set
from pathlib import Path

from pyspark.sql import SparkSession
from py4j.java_gateway import java_import

from app.models.hypergraph import (
    Hypergraph,
    HypergraphNode,
    HypergraphEdge,
    HypertreeNode,
    JoinTreeNode,
    GYOStep
)
from app.hypergraph.plan_to_json import LogicalPlanJsonConverter

logger = logging.getLogger(__name__)


class HypergraphExtractor:
    """Extracts hypergraph structure from SQL queries"""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._scala_bridge_available = self._init_scala_bridge()
        self._plan_converter = LogicalPlanJsonConverter(spark)

    def _init_scala_bridge(self) -> bool:
        """Initialize Scala/JVM bridge for hypergraph extraction"""
        try:
            # Check if custom Spark JAR with hypergraph extraction is available
            jvm = self.spark.sparkContext._jvm

            # Import custom Scala classes for hypergraph extraction
            # This assumes your custom Spark build includes these classes
            java_import(jvm, "org.apache.spark.sql.catalyst.optimizer.NestedColumnAliasing")

            # Try to import the hypergraph extraction utility
            # You may need to adjust the package name based on your Scala code
            try:
                java_import(jvm, "org.apache.spark.sql.catalyst.plans.QueryHypergraph")
                logger.info("Scala hypergraph bridge initialized successfully")
                return True
            except Exception as e:
                logger.warning(f"QueryHypergraph class not found: {e}")
                return False

        except Exception as e:
            logger.warning(f"Could not initialize Scala bridge: {e}")
            return False

    def extract_hypergraph(self, sql: str, query_id: Optional[str] = None, include_schema_attributes: bool = False) -> Hypergraph:
        """
        Extract hypergraph structure from SQL query

        Args:
            sql: SQL query string
            query_id: Optional query identifier
            include_schema_attributes: Whether to include all schema attributes as nodes

        Returns:
            Hypergraph model
        """
        try:
            # Use JSON-based extraction (most reliable)
            return self._extract_via_json(sql, query_id, include_schema_attributes)
        except Exception as e:
            logger.error(f"Error extracting hypergraph via JSON: {e}")
            try:
                # Fallback to string-based extraction
                logger.info("Falling back to string-based extraction")
                return self._extract_via_python(sql, query_id, include_schema_attributes)
            except Exception as e2:
                logger.error(f"Error in fallback extraction: {e2}")
                # Return a basic hypergraph with error information
                return Hypergraph(
                    query_id=query_id,
                    sql=sql,
                    nodes=[],
                    edges=[],
                    is_acyclic=False,
                    is_guarded=False
                )

    def _extract_via_scala(self, sql: str, query_id: Optional[str] = None) -> Hypergraph:
        """
        Extract hypergraph using Scala/JVM code from custom Spark build

        This method calls into the Scala code that analyzes the query plan
        and extracts the hypergraph structure.
        """
        jvm = self.spark.sparkContext._jvm

        # Parse the SQL query to get logical plan
        df = self.spark.sql(sql)
        logical_plan = df._jdf.queryExecution().logical()

        # Call Scala hypergraph extraction
        # This is a placeholder - you'll need to adjust based on your actual Scala API
        try:
            # Example call to hypothetical Scala method:
            # hypergraph_json = jvm.org.apache.spark.sql.catalyst.plans.QueryHypergraph.extract(logical_plan)

            # For now, we'll use Python extraction as fallback
            logger.warning("Scala extraction not fully implemented, using Python fallback")
            return self._extract_via_python(sql, query_id)

        except Exception as e:
            logger.error(f"Scala extraction failed: {e}")
            return self._extract_via_python(sql, query_id)

    def _extract_via_json(self, sql: str, query_id: Optional[str] = None, include_schema_attributes: bool = False) -> Hypergraph:
        """
        Extract hypergraph using JSON representation of Spark logical plan

        In the hypergraph representation:
        - Nodes: Equivalence classes of attributes (joined columns)
        - Edges: Relations/tables containing those attributes
        """
        # Parse query to get DataFrame
        df = self.spark.sql(sql)

        # Get analyzed logical plan as JSON
        plan_json = self._plan_converter.get_analyzed_plan_json(df)

        # Log the plan for debugging
        logger.info(f"Extracted logical plan JSON: {json.dumps(plan_json, indent=2)[:500]}...")

        # Extract output attributes from the top-level plan node
        output_attributes = self._extract_output_attributes(plan_json)
        logger.info(f"Extracted {len(output_attributes)} output attributes from SELECT clause")
        logger.debug(f"Output attribute IDs: {output_attributes}")

        # Extract tables and their attributes
        tables = {}  # Map: table_name -> list of attributes
        filters = []  # List of filter conditions
        aggregates = []  # List of aggregate information

        # Traverse the plan tree to extract components
        self._traverse_plan_json_for_tables(plan_json, tables, filters, aggregates)

        # Extract join predicates from filter conditions
        join_predicates = []
        if filters:
            logger.info(f"Processing {len(filters)} filter conditions to extract join predicates")
            join_predicates = self._extract_join_predicates_from_filters_v2(filters, plan_json, tables)

        # Build equivalence classes of attributes (nodes in hypergraph)
        equivalence_classes = self._build_equivalence_classes(join_predicates, tables)

        # Add singleton nodes for output attributes not involved in joins
        equivalence_classes = self._add_output_attribute_singletons(
            equivalence_classes, tables, output_attributes
        )

        # Optionally add singleton nodes for all other schema attributes
        if include_schema_attributes:
            equivalence_classes = self._add_schema_attribute_singletons(
                equivalence_classes, tables, output_attributes
            )

        # Build hyperedges (one per relation/table)
        hyperedges = self._build_hyperedges(tables, equivalence_classes, output_attributes)

        # Convert equivalence classes to HypergraphNode objects
        nodes = []
        for eq_id, eq_class in enumerate(equivalence_classes):
            # Use representative attribute as label
            representative = sorted(list(eq_class))[0]  # Pick first alphabetically

            # Find output attributes in this equivalence class
            # Match by checking if attribute ends with any output ID (e.g., mi.info#18 ends with #18)
            node_output_attrs = []
            for attr in eq_class:
                for output_id in output_attributes:
                    if attr.endswith(output_id):
                        node_output_attrs.append(attr)
                        break

            # Determine node type
            is_singleton = len(eq_class) == 1
            is_output = len(node_output_attrs) > 0

            # Classify node type:
            # - output_attribute: singleton with output attribute (e.g., mi.info, t.title)
            # - schema_attribute: singleton without output, only when include_schema_attributes=True
            # - attribute: multi-attribute equivalence class (join attributes) or legacy singletons
            if is_singleton and is_output:
                node_type = "output_attribute"
            elif is_singleton and not is_output and include_schema_attributes:
                node_type = "schema_attribute"
            else:
                node_type = "attribute"

            nodes.append(HypergraphNode(
                id=f"attr_{eq_id}",
                label=representative,
                type=node_type,
                attributes=list(eq_class),
                output_attributes=node_output_attrs
            ))

        # Check if acyclic and compute join tree if acyclic
        is_acyclic, join_tree, gyo_steps = self._check_acyclic_advanced(nodes, hyperedges)

        # Check if guarded and determine guardedness type
        is_guarded, guardedness_type = self._check_guardedness(nodes, hyperedges)

        # Calculate complexity metrics
        complexity_metrics = self._calculate_complexity_metrics(
            nodes, hyperedges, tables, join_predicates, aggregates,
            is_acyclic, join_tree, gyo_steps
        )

        return Hypergraph(
            query_id=query_id,
            sql=sql,
            nodes=nodes,
            edges=hyperedges,
            is_acyclic=is_acyclic,
            is_guarded=is_guarded,
            guardedness_type=guardedness_type,
            join_tree=join_tree,
            gyo_steps=gyo_steps,
            num_relations=len(tables),
            num_joins=len(join_predicates),
            num_aggregates=len(aggregates),
            complexity_metrics=complexity_metrics
        )

    def _extract_output_attributes(self, plan_json: Dict[str, Any]) -> set:
        """
        Extract output attributes from the SELECT clause (top-level plan node)

        For queries with aggregates, we also extract the input attributes used in
        aggregate functions (e.g., for MIN(mi.info), we extract mi.info's ID).

        Args:
            plan_json: The logical plan JSON

        Returns:
            Set of output attribute IDs (in format #id)
        """
        output_attrs = set()

        # The top-level node contains the output columns
        if "output" in plan_json:
            for attr in plan_json["output"]:
                attr_name = attr.get("name", "")
                expr_id = attr.get("exprId", "")

                # Extract numeric ID from exprId (format: "ExprId(123)")
                if expr_id:
                    id_match = re.search(r'\d+', expr_id)
                    if id_match:
                        output_attrs.add(f"#{id_match.group()}")

        # Also extract input attributes from aggregate functions
        # For example: "min(info#9) AS budget#4" -> extract #9
        aggregate_inputs = self._extract_aggregate_input_attributes(plan_json)
        output_attrs.update(aggregate_inputs)

        logger.info(f"Found {len(output_attrs)} output attribute IDs (including aggregate inputs)")
        return output_attrs

    def _extract_aggregate_input_attributes(self, plan_node: Dict[str, Any]) -> set:
        """
        Recursively find Aggregate nodes and extract input attribute IDs from aggregate expressions

        Args:
            plan_node: Current node in the plan tree

        Returns:
            Set of attribute IDs used as inputs to aggregate functions
        """
        input_attrs = set()

        # Check if this is an Aggregate node
        if plan_node.get("nodeType") == "Aggregate":
            metadata = plan_node.get("metadata", {})
            aggregate_exprs = metadata.get("aggregates", [])

            for agg_expr in aggregate_exprs:
                # Parse expressions like "min(info#9) AS budget#4" or "count(DISTINCT movie_id#10)"
                # Extract all attribute IDs (format: word#number)
                attr_ids = re.findall(r'(\w+)#(\d+)', agg_expr)
                for attr_name, attr_id in attr_ids:
                    input_attrs.add(f"#{attr_id}")
                    logger.debug(f"Found aggregate input: {attr_name}#{attr_id} in expression: {agg_expr}")

        # Recursively traverse children
        for child in plan_node.get("children", []):
            input_attrs.update(self._extract_aggregate_input_attributes(child))

        return input_attrs

    def _traverse_plan_json_for_tables(
        self,
        plan_node: Dict[str, Any],
        tables: Dict[str, List[str]],
        filters: List[Dict[str, Any]],
        aggregates: List[Dict[str, Any]]
    ) -> None:
        """
        Recursively traverse JSON plan tree to extract tables and their attributes

        Args:
            plan_node: Current node in the plan tree
            tables: Dict to populate with table names and their attributes
            filters: List to populate with filter conditions
            aggregates: List to populate with aggregate information
        """
        node_type = plan_node.get("nodeType", "")
        node_name = plan_node.get("nodeName", "")
        metadata = plan_node.get("metadata", {})

        # Extract tables and their attributes
        if node_type == "SubqueryAlias":
            # Extract table name from nodeName (format: "Table: alias_name")
            table_name = None
            if ":" in node_name:
                table_name = node_name.split(":")[-1].strip()
            else:
                table_name = metadata.get("alias", "")

            # Only extract from outer aliases (have SubqueryAlias child)
            children = plan_node.get("children", [])
            has_subquery_child = len(children) > 0 and children[0].get("nodeType") == "SubqueryAlias"

            if has_subquery_child and table_name and table_name not in tables:
                # Get attributes with their expression IDs
                attributes = []
                for attr in plan_node.get("output", []):
                    attr_name = attr.get("name", "")
                    expr_id = attr.get("exprId", "")
                    if attr_name and expr_id:
                        # Extract numeric ID from exprId (format: "ExprId(123)")
                        id_match = re.search(r'\d+', expr_id)
                        if id_match:
                            # Store as "table.attr#id"
                            full_attr = f"{table_name}.{attr_name}#{id_match.group()}"
                            attributes.append(full_attr)

                tables[table_name] = attributes
                logger.info(f"Extracted table '{table_name}' with {len(attributes)} attributes")

        # Extract aggregate operations
        elif node_type == "Aggregate":
            group_by = metadata.get("groupBy", [])
            aggregate_funcs = metadata.get("aggregates", [])

            aggregate_info = {
                "type": "aggregate",
                "groupBy": group_by,
                "aggregates": aggregate_funcs
            }
            aggregates.append(aggregate_info)
            logger.info(f"Extracted aggregate with {len(group_by)} GROUP BY columns and {len(aggregate_funcs)} aggregate functions")

        # Extract filter conditions
        elif node_type == "Filter":
            filter_condition = metadata.get("condition", "")

            if filter_condition:
                filter_info = {
                    "type": "filter",
                    "condition": filter_condition
                }
                filters.append(filter_info)
                logger.info(f"Extracted filter condition: {filter_condition[:100]}")

        # Extract join conditions from Join nodes
        elif node_type == "Join":
            join_condition = metadata.get("condition", "")

            if join_condition:
                filter_info = {
                    "type": "join",
                    "condition": join_condition
                }
                filters.append(filter_info)
                logger.info(f"Extracted join condition: {join_condition[:100]}")

        # Recursively traverse children
        for child in plan_node.get("children", []):
            self._traverse_plan_json_for_tables(child, tables, filters, aggregates)

    def _extract_columns_from_condition(self, condition: str) -> List[str]:
        """
        Extract column references from a join or filter condition

        Args:
            condition: String representation of the condition

        Returns:
            List of column names referenced in the condition
        """
        # Pattern to match column references like "table.column" or "column#id"
        # This is a simplified pattern - actual Spark expressions are more complex
        column_pattern = r'(\w+(?:#\d+)?)'

        columns = []
        matches = re.finditer(column_pattern, condition)

        for match in matches:
            col = match.group(1)
            # Filter out SQL keywords
            if col.lower() not in ['and', 'or', 'not', 'null', 'true', 'false', 'is']:
                columns.append(col)

        return columns

    def _get_tables_from_subtree(self, plan_node: Dict[str, Any]) -> Set[str]:
        """
        Extract all table names referenced in a subtree of the plan

        Args:
            plan_node: Root of the subtree

        Returns:
            Set of table names found in the subtree
        """
        tables = set()

        node_type = plan_node.get("nodeType", "")
        node_name = plan_node.get("nodeName", "")
        metadata = plan_node.get("metadata", {})

        # Extract from SubqueryAlias (primary table reference)
        if node_type == "SubqueryAlias":
            # Extract table name from nodeName (format: "Table: alias_name")
            table_name = None
            if ":" in node_name:
                table_name = node_name.split(":")[-1].strip()
            else:
                table_name = metadata.get("alias", "")

            # Only extract from nodes with SubqueryAlias children (outer aliases)
            children = plan_node.get("children", [])
            has_subquery_child = len(children) > 0 and children[0].get("nodeType") == "SubqueryAlias"

            if has_subquery_child and table_name:
                tables.add(table_name)

        # Recursively check children
        for child in plan_node.get("children", []):
            tables.update(self._get_tables_from_subtree(child))

        return tables

    def _count_aggregates_in_plan(self, plan_node: Dict[str, Any]) -> int:
        """Count aggregate operations in the plan tree"""
        count = 0

        if plan_node.get("nodeType") == "Aggregate":
            count += 1

        for child in plan_node.get("children", []):
            count += self._count_aggregates_in_plan(child)

        return count

    def _extract_join_predicates_from_filters(
        self,
        filters: List[Dict[str, Any]],
        relations: Dict[str, HypergraphNode],
        plan_json: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Extract join predicates from filter conditions

        Parse filter conditions to identify equality predicates that join tables.

        Args:
            filters: List of filter information
            relations: Extracted relations
            plan_json: Full plan JSON for output analysis

        Returns:
            List of join predicates with table references
        """
        # Build column-to-table mapping from plan outputs
        column_to_table = self._build_column_to_table_mapping(plan_json, relations)

        join_predicates = []

        for filter_info in filters:
            condition = filter_info.get("condition", "")
            if not condition:
                continue

            logger.info(f"Parsing filter condition: {condition[:200]}")

            # Split condition by AND to get individual predicates
            # Pattern: Match equality predicates like "col1#123 = col2#456"
            equality_pattern = r'(\w+#\d+)\s*=\s*(\w+#\d+)'

            for match in re.finditer(equality_pattern, condition):
                left_col = match.group(1)
                right_col = match.group(2)

                # Determine which tables these columns belong to
                left_table = column_to_table.get(left_col)
                right_table = column_to_table.get(right_col)

                # If columns are from different tables, this is a join predicate
                if left_table and right_table and left_table != right_table:
                    join_pred = {
                        "left_column": left_col,
                        "right_column": right_col,
                        "left_table": left_table,
                        "right_table": right_table,
                        "predicate": f"{left_col} = {right_col}"
                    }
                    join_predicates.append(join_pred)
                    logger.info(f"Found join predicate: {left_table}.{left_col} = {right_table}.{right_col}")

        return join_predicates

    def _build_column_to_table_mapping(
        self,
        plan_node: Dict[str, Any],
        relations: Dict[str, HypergraphNode],
        column_to_table: Optional[Dict[str, str]] = None
    ) -> Dict[str, str]:
        """
        Build a mapping from column IDs (with #) to table names

        Recursively traverse the plan and map each column reference to its source table.
        """
        if column_to_table is None:
            column_to_table = {}

        node_type = plan_node.get("nodeType", "")
        metadata = plan_node.get("metadata", {})

        # For SubqueryAlias nodes, map output columns to the table
        if node_type == "SubqueryAlias":
            # Extract table name from nodeName (format: "Table: alias_name")
            node_name = plan_node.get("nodeName", "")
            table_name = None
            if ":" in node_name:
                table_name = node_name.split(":")[-1].strip()
            else:
                table_name = metadata.get("alias", "")

            # Only map from nodes with SubqueryAlias children (outer aliases)
            children = plan_node.get("children", [])
            has_subquery_child = len(children) > 0 and children[0].get("nodeType") == "SubqueryAlias"

            if has_subquery_child and table_name in relations:
                # Map all output columns to this table
                for attr in plan_node.get("output", []):
                    col_name = attr.get("name", "")
                    expr_id = attr.get("exprId", "")

                    # Build column identifier in Spark format: name#id
                    if col_name and expr_id:
                        # Extract numeric ID from exprId (format: "ExprId(123)")
                        id_match = re.search(r'\d+', expr_id)
                        if id_match:
                            col_id = f"{col_name}#{id_match.group()}"
                            column_to_table[col_id] = table_name

        # Recursively process children
        for child in plan_node.get("children", []):
            self._build_column_to_table_mapping(child, relations, column_to_table)

        return column_to_table

    def _associate_predicates_with_edges(
        self,
        join_predicates: List[Dict[str, Any]],
        joins: List[HypergraphEdge],
        relations: Dict[str, HypergraphNode]
    ) -> None:
        """
        Associate extracted join predicates with hyperedges

        Match predicates to existing hyperedges based on which tables are involved.
        """
        for predicate in join_predicates:
            left_table = predicate["left_table"]
            right_table = predicate["right_table"]
            pred_str = predicate["predicate"]

            # Find hyperedge that connects these tables
            matched = False
            for edge in joins:
                edge_tables = set(edge.nodes)
                # Check if this edge connects the two tables involved in the predicate
                if left_table in edge_tables and right_table in edge_tables:
                    # Add predicate to this edge if not already present
                    if pred_str not in edge.join_conditions:
                        edge.join_conditions.append(pred_str)
                        logger.info(f"Associated predicate '{pred_str}' with edge {edge.id}")
                    matched = True
                    break

            # If no existing edge connects these tables, create a new one
            if not matched and left_table in relations and right_table in relations:
                new_edge = HypergraphEdge(
                    id=f"join_{len(joins)}",
                    nodes=[left_table, right_table],
                    join_conditions=[pred_str],
                    label=f"Join on {pred_str}"
                )
                joins.append(new_edge)
                logger.info(f"Created new hyperedge for predicate: {pred_str}")

    def _check_guardedness(
        self,
        nodes: List[HypergraphNode],
        edges: List[HypergraphEdge]
    ) -> tuple[bool, str]:
        """
        Check if the query is guarded, piecewise-guarded, or unguarded.

        A query with aggregates is:
        - GUARDED: if there exists a single relation (guard) that contains
          all output/free variables (attributes in SELECT/GROUP BY)
        - PIECEWISE_GUARDED: if multiple relations together cover all output
          variables, but no single relation covers them all
        - UNGUARDED: if no combination of relations covers all output variables

        Returns:
            Tuple of (is_guarded: bool, guardedness_type: str)
            guardedness_type is one of: "guarded", "piecewise_guarded", "unguarded"
        """
        if len(edges) == 0:
            # No relations means trivially guarded
            return True, "guarded"

        # Collect all output attribute node IDs
        # Output attributes are nodes of type "output_attribute" or nodes with output_attributes
        output_node_ids = set()
        for node in nodes:
            if node.type == "output_attribute":
                output_node_ids.add(node.id)
            elif node.output_attributes:
                output_node_ids.add(node.id)

        # If no output attributes, query is trivially guarded
        if not output_node_ids:
            logger.info("No output attributes found - trivially guarded")
            return True, "guarded"

        logger.info(f"Output node IDs for guardedness check: {output_node_ids}")

        # Check each edge to see if it covers all output nodes
        for edge in edges:
            edge_node_set = set(edge.nodes)
            if output_node_ids.issubset(edge_node_set):
                logger.info(f"Edge {edge.id} is a guard - covers all output attributes")
                return True, "guarded"

        # No single guard found - check if piecewise guarded
        # Build coverage: which output nodes are covered by which edges
        covered_by_any = set()
        for edge in edges:
            edge_node_set = set(edge.nodes)
            covered_by_any.update(edge_node_set & output_node_ids)

        if output_node_ids.issubset(covered_by_any):
            # All output attributes are covered by some edge(s)
            logger.info("Piecewise guarded - multiple edges together cover all output attributes")
            return False, "piecewise_guarded"
        else:
            # Some output attributes not covered by any edge
            uncovered = output_node_ids - covered_by_any
            logger.info(f"Unguarded - output attributes not in any edge: {uncovered}")
            return False, "unguarded"

    def _check_acyclic_advanced(
        self,
        nodes: List[HypergraphNode],
        edges: List[HypergraphEdge]
    ) -> tuple[bool, Optional[List[JoinTreeNode]], Optional[List[GYOStep]]]:
        """
        Check if hypergraph is acyclic using the GYO algorithm
        and compute join tree if acyclic

        A hypergraph is acyclic if it can be reduced to empty by repeatedly:
        1. Removing an edge that is contained in another edge (ear removal)
        2. Removing a node that appears in only one edge (node removal)

        This is the standard definition used in database theory.

        Returns:
            Tuple of (is_acyclic, join_tree, gyo_steps)
            join_tree is None if not acyclic
            gyo_steps contains the step-by-step reduction for visualization
        """
        if len(edges) == 0:
            return True, None, []

        if len(nodes) == 0:
            return len(edges) == 0, None, []

        # Work with copies since we'll be modifying them
        # Map edge index to original edge for tracking
        edge_map = {i: edges[i] for i in range(len(edges))}
        remaining_edges = {i: set(edge.nodes) for i, edge in enumerate(edges)}
        remaining_nodes = set(node.id for node in nodes)

        # Track the order of edge removal for join tree construction
        # Each entry: (edge_index, level, connected_edge_indices)
        removal_order = []
        current_level = 0

        # Track GYO steps for visualization
        gyo_steps = []
        step_number = 0

        # GYO reduction
        changed = True
        while changed and remaining_edges:
            changed = False
            edges_removed_this_iteration = []

            # Step 1: Remove ears (edges contained in other edges)
            edges_to_remove = []
            ear_details = {}  # Track container information for each ear
            ear_nodes_backup = {}  # Store nodes before deletion

            for i, edge1 in remaining_edges.items():
                containers = []
                for j, edge2 in remaining_edges.items():
                    if i != j and edge1.issubset(edge2):
                        containers.append(j)
                        if i not in edges_to_remove:
                            edges_to_remove.append(i)
                            changed = True

                # Store detailed container information and nodes
                if containers:
                    ear_details[i] = containers
                    ear_nodes_backup[i] = edge1.copy()

            # Before removing, record which edges each removed edge is connected to
            for idx in edges_to_remove:
                # Find all remaining edges (except this one) that share nodes with this edge
                connected_edges = []
                edge_nodes = remaining_edges[idx]
                for other_idx, other_edge in remaining_edges.items():
                    if other_idx != idx and edge_nodes.intersection(other_edge):
                        connected_edges.append(other_idx)

                removal_order.append((idx, current_level, connected_edges))
                logger.debug(f"GYO: Removing ear edge {remaining_edges[idx]}, connected to {connected_edges}")
                edges_removed_this_iteration.append(idx)
                del remaining_edges[idx]

            if changed:
                # Record GYO step with enhanced subset relationship data
                removed_edge_names = [edge_map[idx].id for idx in edges_to_remove]
                remaining_edge_names = [edge_map[idx].id for idx in remaining_edges.keys()]

                # Build detailed subset relationship information
                all_container_edges = []
                all_affected_nodes = []
                subset_relationships = []
                why_ear_parts = []

                for ear_idx in edges_to_remove:
                    ear_edge = edge_map[ear_idx]
                    # Use backed-up nodes (captured before deletion)
                    original_ear_nodes = ear_nodes_backup.get(ear_idx, set())

                    all_affected_nodes.extend(list(original_ear_nodes))

                    # Get containers for this ear
                    container_idxs = ear_details.get(ear_idx, [])
                    container_names = [edge_map[c_idx].id for c_idx in container_idxs]
                    all_container_edges.extend(container_names)

                    # Build why_ear explanation
                    if container_names:
                        why_ear_parts.append(f"{ear_edge.id} ⊆ {{{', '.join(container_names)}}}")

                    # Build detailed subset relationships
                    for c_idx in container_idxs:
                        container_edge = edge_map[c_idx]
                        container_nodes = remaining_edges.get(c_idx, set())

                        # Find shared and extra nodes
                        shared = original_ear_nodes.intersection(container_nodes) if container_nodes else original_ear_nodes
                        extra_in_container = container_nodes - original_ear_nodes if container_nodes else set()

                        subset_relationships.append({
                            "ear": ear_edge.id,
                            "container": container_edge.id,
                            "ear_nodes": list(original_ear_nodes),
                            "container_nodes": list(container_nodes),
                            "shared_nodes": list(shared),
                            "extra_in_container": list(extra_in_container)
                        })

                # Create why_ear explanation
                why_ear = " and ".join(why_ear_parts) if why_ear_parts else None

                gyo_steps.append(GYOStep(
                    step_number=step_number,
                    action="remove_ear",
                    description=f"Removed ear edges: {', '.join(removed_edge_names)} (contained in other edges)",
                    removed_edges=removed_edge_names,
                    removed_nodes=[],
                    remaining_edges=remaining_edge_names,
                    remaining_nodes=list(remaining_nodes),
                    # Phase 3 enhancements
                    why_ear=why_ear,
                    container_edges=list(set(all_container_edges)),  # Deduplicate
                    affected_nodes=list(set(all_affected_nodes)),  # Deduplicate
                    subset_relationships=subset_relationships
                ))
                step_number += 1

                current_level += 1
                continue

            # Step 2: Remove nodes that appear in only one edge
            if remaining_edges:
                # Count node occurrences
                node_count = {}
                for edge in remaining_edges.values():
                    for node in edge:
                        node_count[node] = node_count.get(node, 0) + 1

                # Find nodes that appear in only one edge
                single_occurrence_nodes = [node for node, count in node_count.items() if count == 1]

                if single_occurrence_nodes:
                    # Find edges that contain single-occurrence nodes
                    edges_with_single_nodes = set()
                    for node in single_occurrence_nodes:
                        for edge_idx, edge in remaining_edges.items():
                            if node in edge:
                                edges_with_single_nodes.add(edge_idx)

                    # BEFORE removing nodes, record which edges will become empty and what they connect to
                    edges_to_connections = {}
                    for edge_idx in edges_with_single_nodes:
                        # Record connections before modification
                        edge_nodes = remaining_edges[edge_idx]
                        connected_edges = []
                        for other_idx, other_edge in remaining_edges.items():
                            if other_idx != edge_idx and edge_nodes.intersection(other_edge):
                                connected_edges.append(other_idx)
                        edges_to_connections[edge_idx] = connected_edges

                    # Remove these nodes from all edges
                    for node in single_occurrence_nodes:
                        for edge in remaining_edges.values():
                            if node in edge:
                                edge.discard(node)
                        logger.debug(f"GYO: Removed node {node} (appears in only one edge)")

                    # Remove empty edges and track them
                    edges_to_remove = [idx for idx, e in remaining_edges.items() if len(e) == 0]
                    for edge_idx in edges_to_remove:
                        # Use the connections we recorded before node removal
                        connected_edges = edges_to_connections.get(edge_idx, [])
                        # Filter out edges that are also being removed
                        connected_edges = [e for e in connected_edges if e not in edges_to_remove]
                        removal_order.append((edge_idx, current_level, connected_edges))
                        logger.debug(f"GYO: Removing empty edge {edge_idx}, connected to {connected_edges}")
                        edges_removed_this_iteration.append(edge_idx)
                        del remaining_edges[edge_idx]

                    # Always mark as changed when we remove nodes, even if no edges became empty
                    # The hypergraph structure has changed and we need to continue reduction
                    changed = True

                    # Record GYO step (only if edges were actually removed)
                    if edges_removed_this_iteration:

                        removed_edge_names = [edge_map[idx].id for idx in edges_to_remove]
                        remaining_edge_names = [edge_map[idx].id for idx in remaining_edges.keys()]

                        gyo_steps.append(GYOStep(
                            step_number=step_number,
                            action="remove_node",
                            description=f"Removed nodes appearing in only one edge: {', '.join(single_occurrence_nodes)}. Removed empty edges: {', '.join(removed_edge_names)}",
                            removed_edges=removed_edge_names,
                            removed_nodes=single_occurrence_nodes,
                            remaining_edges=remaining_edge_names,
                            remaining_nodes=list(remaining_nodes)
                        ))
                        step_number += 1

                        current_level += 1

        # Hypergraph is acyclic if we reduced it to empty
        is_acyclic = len(remaining_edges) == 0

        if is_acyclic:
            logger.info("Hypergraph is ACYCLIC (GYO reduction successful)")
            # Build join tree from removal order
            join_tree = self._build_join_tree(edges, removal_order, nodes)
            return True, join_tree, gyo_steps
        else:
            logger.info(f"Hypergraph is CYCLIC (could not reduce, {len(remaining_edges)} edges remaining)")
            return False, None, gyo_steps

    def _build_join_tree(
        self,
        edges: List[HypergraphEdge],
        removal_order: List[tuple],
        nodes: List[HypergraphNode]
    ) -> List[JoinTreeNode]:
        """
        Build a join tree from the GYO reduction order

        The join tree shows a valid order for executing joins in an acyclic query.
        Edges removed later in the GYO algorithm are ancestors of edges removed earlier.

        Args:
            edges: Original hyperedges
            removal_order: List of (edge_index, level, connected_edges) tuples from GYO reduction
            nodes: Hypergraph nodes for attribute information

        Returns:
            List of JoinTreeNode objects representing the join tree
        """
        if not removal_order:
            return []

        # Create node attributes map
        node_attrs_map = {node.id: node.attributes for node in nodes}

        # Create lookup maps
        edge_to_tree_node = {}  # Map edge index to JoinTreeNode id
        tree_node_lookup = {}   # Map tree node id to JoinTreeNode

        # First pass: Create all tree nodes
        for edge_idx, level, connected_edges in removal_order:
            edge = edges[edge_idx]
            tree_node_id = f"join_tree_{edge.id}"

            # Get attributes for this relation
            attrs = node_attrs_map.get(edge.id, [])

            # Create join tree node
            join_tree_node = JoinTreeNode(
                id=tree_node_id,
                relation=edge.id,
                attributes=attrs,
                level=level,
                children=[],
                parent=None
            )

            edge_to_tree_node[edge_idx] = tree_node_id
            tree_node_lookup[tree_node_id] = join_tree_node

        # Second pass: Assign parents based on connected_edges
        # Process in order from first removed to last removed
        for edge_idx, level, connected_edges in removal_order:
            tree_node_id = edge_to_tree_node[edge_idx]
            tree_node = tree_node_lookup[tree_node_id]

            if tree_node.parent is not None:
                # Already has a parent
                continue

            if not connected_edges:
                # No connections - this will be a root
                continue

            # Find the best parent: prefer edges removed later (higher level)
            # Among the connected edges, choose one that was removed after this one
            best_parent_idx = None
            best_parent_level = -1

            for conn_edge_idx in connected_edges:
                if conn_edge_idx in edge_to_tree_node:
                    conn_tree_node = tree_node_lookup[edge_to_tree_node[conn_edge_idx]]
                    if conn_tree_node.level > best_parent_level:
                        best_parent_level = conn_tree_node.level
                        best_parent_idx = conn_edge_idx

            # Assign parent
            if best_parent_idx is not None:
                parent_tree_node_id = edge_to_tree_node[best_parent_idx]
                parent_tree_node = tree_node_lookup[parent_tree_node_id]

                tree_node.parent = parent_tree_node_id
                if tree_node_id not in parent_tree_node.children:
                    parent_tree_node.children.append(tree_node_id)

                # Compute shared attributes
                edge_nodes = set(edges[edge_idx].nodes)
                parent_edge_nodes = set(edges[best_parent_idx].nodes)
                shared_nodes = edge_nodes.intersection(parent_edge_nodes)

                shared_attrs = []
                for node_id in shared_nodes:
                    if node_id in node_attrs_map:
                        shared_attrs.extend(node_attrs_map[node_id])

                parent_tree_node.shared_attributes[tree_node_id] = shared_attrs
                logger.debug(f"Assigned parent {parent_tree_node.relation} to {tree_node.relation}, shared: {shared_attrs}")

        join_tree_nodes = list(tree_node_lookup.values())
        logger.info(f"Built join tree with {len(join_tree_nodes)} nodes")

        # Debug: check how many roots we have
        roots = [node for node in join_tree_nodes if node.parent is None]
        if len(roots) > 1:
            logger.warning(f"Join tree has {len(roots)} roots: {[r.relation for r in roots]}")
        else:
            logger.info(f"Join tree has single root: {roots[0].relation if roots else 'none'}")

        return join_tree_nodes

    def _extract_join_predicates_from_filters_v2(
        self,
        filters: List[Dict[str, Any]],
        plan_json: Dict[str, Any],
        tables: Dict[str, List[str]]
    ) -> List[tuple]:
        """
        Extract join predicates from filter conditions

        Returns list of tuples: (attr1, attr2) where attr1 and attr2 are joined
        Format: "table.column#id"
        """
        join_predicates = []

        for filter_info in filters:
            condition = filter_info.get("condition", "")
            if not condition:
                continue

            logger.info(f"Parsing filter condition: {condition[:200]}")

            # Pattern: Match equality predicates like "col1#123 = col2#456"
            equality_pattern = r'(\w+)#(\d+)\s*=\s*(\w+)#(\d+)'

            for match in re.finditer(equality_pattern, condition):
                col1_name = match.group(1)
                col1_id = match.group(2)
                col2_name = match.group(3)
                col2_id = match.group(4)

                # Find which tables these columns belong to
                attr1 = self._find_attribute(col1_name, col1_id, tables)
                attr2 = self._find_attribute(col2_name, col2_id, tables)

                if attr1 and attr2 and attr1 != attr2:
                    join_predicates.append((attr1, attr2))
                    logger.info(f"Found join predicate: {attr1} = {attr2}")

        return join_predicates

    def _find_attribute(self, col_name: str, col_id: str, tables: Dict[str, List[str]]) -> Optional[str]:
        """Find full attribute name (table.col#id) given column name and ID"""
        target = f"#{col_id}"
        for table_name, attributes in tables.items():
            for attr in attributes:
                if attr.endswith(target) and col_name in attr:
                    return attr
        return None

    def _build_equivalence_classes(
        self,
        join_predicates: List[tuple],
        tables: Dict[str, List[str]]
    ) -> List[Set[str]]:
        """
        Build equivalence classes of attributes using union-find

        Each equivalence class represents attributes that are joined together.

        IMPORTANT: Only attributes involved in joins are included in equivalence classes.
        Non-join attributes (e.g., projected or filtered columns) are excluded from the
        hypergraph to ensure correct acyclicity detection via GYO algorithm.
        """
        # Collect only attributes that appear in join predicates
        # This is crucial: including non-join attributes creates singleton equivalence
        # classes that prevent the GYO algorithm from recognizing ears correctly
        join_attributes = set()
        for attr1, attr2 in join_predicates:
            join_attributes.add(attr1)
            join_attributes.add(attr2)

        logger.info(f"Found {len(join_attributes)} attributes involved in {len(join_predicates)} join predicates")

        # Union-find data structure (only for attributes involved in joins)
        parent = {attr: attr for attr in join_attributes}

        def find(x):
            if parent[x] != x:
                parent[x] = find(parent[x])  # Path compression
            return parent[x]

        def union(x, y):
            root_x = find(x)
            root_y = find(y)
            if root_x != root_y:
                parent[root_x] = root_y

        # Union attributes based on join predicates
        for attr1, attr2 in join_predicates:
            union(attr1, attr2)

        # Group attributes by their root
        equivalence_map = {}
        for attr in join_attributes:
            root = find(attr)
            if root not in equivalence_map:
                equivalence_map[root] = set()
            equivalence_map[root].add(attr)

        equivalence_classes = list(equivalence_map.values())

        logger.info(f"Built {len(equivalence_classes)} equivalence classes from {len(join_predicates)} join predicates")
        for i, eq_class in enumerate(equivalence_classes):
            logger.info(f"  Class {i}: {eq_class}")

        return equivalence_classes

    def _add_output_attribute_singletons(
        self,
        equivalence_classes: List[Set[str]],
        tables: Dict[str, List[str]],
        output_attribute_ids: set
    ) -> List[Set[str]]:
        """
        Add singleton equivalence classes for output attributes not involved in joins

        This makes output attributes (like mi.info, t.title) visible as nodes in the graph,
        even if they're not used in join conditions.

        Args:
            equivalence_classes: Existing equivalence classes (from join predicates)
            tables: Map of table names to their attributes
            output_attribute_ids: Set of attribute IDs that appear in SELECT/aggregates

        Returns:
            Extended list of equivalence classes including singletons for output attributes
        """
        # Find all attributes already in equivalence classes
        existing_attrs = set()
        for eq_class in equivalence_classes:
            existing_attrs.update(eq_class)

        # Find output attributes not yet in any equivalence class
        singleton_count = 0
        for table_name, attributes in tables.items():
            for attr in attributes:
                # Check if this is an output attribute
                is_output = False
                for output_id in output_attribute_ids:
                    if attr.endswith(output_id):
                        is_output = True
                        break

                # If it's an output attribute and not already in a class, create singleton
                if is_output and attr not in existing_attrs:
                    equivalence_classes.append({attr})
                    singleton_count += 1
                    logger.info(f"Created singleton node for output attribute: {attr}")

        if singleton_count > 0:
            logger.info(f"Added {singleton_count} singleton nodes for output attributes")

        return equivalence_classes

    def _add_schema_attribute_singletons(
        self,
        equivalence_classes: List[Set[str]],
        tables: Dict[str, List[str]],
        output_attribute_ids: set
    ) -> List[Set[str]]:
        """
        Add singleton equivalence classes for schema attributes not involved in joins or outputs

        This makes all table attributes visible as nodes in the graph, including non-join,
        non-output columns like cn.name, cn.country_code, etc.

        Args:
            equivalence_classes: Existing equivalence classes (from join predicates and output attrs)
            tables: Map of table names to their attributes
            output_attribute_ids: Set of attribute IDs that appear in SELECT/aggregates

        Returns:
            Extended list of equivalence classes including singletons for schema attributes
        """
        # Find all attributes already in equivalence classes
        existing_attrs = set()
        for eq_class in equivalence_classes:
            existing_attrs.update(eq_class)

        # Find schema attributes not yet in any equivalence class (not join, not output)
        singleton_count = 0
        for table_name, attributes in tables.items():
            for attr in attributes:
                # Check if this is an output attribute
                is_output = False
                for output_id in output_attribute_ids:
                    if attr.endswith(output_id):
                        is_output = True
                        break

                # If it's NOT an output attribute and NOT already in a class, create singleton
                if not is_output and attr not in existing_attrs:
                    equivalence_classes.append({attr})
                    singleton_count += 1
                    logger.info(f"Created singleton node for schema attribute: {attr}")

        if singleton_count > 0:
            logger.info(f"Added {singleton_count} singleton nodes for schema attributes")

        return equivalence_classes

    def _build_hyperedges(
        self,
        tables: Dict[str, List[str]],
        equivalence_classes: List[Set[str]],
        output_attribute_ids: set
    ) -> List[HypergraphEdge]:
        """
        Build hyperedges (one per table/relation)

        Each hyperedge connects the equivalence classes that contain JOIN attributes from that table.
        All attributes (including non-join) are stored in the attributes field for visualization.

        Args:
            tables: Map of table names to their attributes
            equivalence_classes: List of equivalence classes (sets of attributes)
            output_attribute_ids: Set of attribute IDs that appear in SELECT clause (format: #123)

        Returns:
            List of HypergraphEdge objects
        """
        # Create mapping from attribute to equivalence class ID
        attr_to_class = {}
        for eq_id, eq_class in enumerate(equivalence_classes):
            for attr in eq_class:
                attr_to_class[attr] = f"attr_{eq_id}"

        hyperedges = []
        for table_name, attributes in tables.items():
            # Find which equivalence classes this table's JOIN attributes belong to
            connected_classes = set()
            for attr in attributes:
                if attr in attr_to_class:
                    connected_classes.add(attr_to_class[attr])

            # Identify which attributes from this table are output attributes
            table_output_attrs = []
            for attr in attributes:
                # Check if this attribute's ID matches any output attribute ID
                for output_id in output_attribute_ids:
                    if attr.endswith(output_id):
                        table_output_attrs.append(attr)
                        logger.debug(f"Matched output attribute: {attr} ends with {output_id}")
                        break

            if len(attributes) > 0 and len(output_attribute_ids) > 0 and len(table_output_attrs) == 0:
                logger.debug(f"Table {table_name} has {len(attributes)} attributes but no output attributes matched")
                logger.debug(f"  Table attributes: {attributes[:3]}")
                logger.debug(f"  Output IDs: {list(output_attribute_ids)[:5]}")

            # Always create a hyperedge for each table (even if no connected classes)
            # Tables without join attributes will have empty nodes list
            hyperedges.append(HypergraphEdge(
                id=table_name,
                nodes=list(connected_classes) if connected_classes else [],
                join_conditions=[],
                label=table_name,
                attributes=attributes,  # Store ALL attributes (including non-join) for visualization
                output_attributes=table_output_attrs  # Store output attributes
            ))
            logger.info(f"Created hyperedge for table '{table_name}' connecting nodes: {connected_classes if connected_classes else '(no joins)'}, total attributes: {len(attributes)}, output attributes: {len(table_output_attrs)}")

        return hyperedges

    def _extract_via_python(self, sql: str, query_id: Optional[str] = None, include_schema_attributes: bool = False) -> Hypergraph:
        """
        Extract hypergraph using Python analysis of Spark logical plan

        This is a simplified version that parses the logical plan string.
        For accurate hypergraph extraction, the Scala bridge should be used.
        """
        # Parse query to get logical plan
        df = self.spark.sql(sql)
        logical_plan = df._jdf.queryExecution().logical().toString()

        # Extract relations (tables)
        nodes = self._extract_relations(logical_plan)

        # Extract joins
        edges = self._extract_joins(logical_plan, nodes)

        # Determine if acyclic and compute join tree if acyclic
        is_acyclic, join_tree, gyo_steps = self._check_acyclic_advanced(nodes, edges)

        return Hypergraph(
            query_id=query_id,
            sql=sql,
            nodes=nodes,
            edges=edges,
            is_acyclic=is_acyclic,
            is_guarded=False,  # Would need deeper analysis
            join_tree=join_tree,
            gyo_steps=gyo_steps,
            num_relations=len(nodes),
            num_joins=len(edges),
            num_aggregates=logical_plan.count("Aggregate")
        )

    def _extract_relations(self, logical_plan: str) -> list[HypergraphNode]:
        """Extract relations/tables from logical plan"""
        nodes = []

        # Simple parsing - look for Relation nodes
        # Format: Relation[attr1#1, attr2#2] <table_name>
        import re
        relation_pattern = r'Relation\[([^\]]+)\]\s+(\w+)'

        for match in re.finditer(relation_pattern, logical_plan):
            attributes_str = match.group(1)
            table_name = match.group(2)

            # Parse attributes
            attributes = [attr.split('#')[0] for attr in attributes_str.split(',')]

            nodes.append(HypergraphNode(
                id=table_name,
                label=table_name,
                type="relation",
                attributes=attributes
            ))

        return nodes

    def _extract_joins(
        self,
        logical_plan: str,
        nodes: list[HypergraphNode]
    ) -> list[HypergraphEdge]:
        """Extract join conditions to form hyperedges"""
        edges = []

        # Simple parsing - look for Join nodes
        # This is very simplified and may need refinement
        import re

        join_pattern = r'Join\s+(\w+)'
        join_matches = list(re.finditer(join_pattern, logical_plan))

        for i, match in enumerate(join_matches):
            join_type = match.group(1)

            # In a real implementation, we'd parse the join conditions
            # to determine which tables are involved
            # For now, create a simple edge

            # Get adjacent relations (simplified)
            edge_nodes = [node.id for node in nodes[:min(2, len(nodes))]]

            if len(edge_nodes) >= 2:
                edges.append(HypergraphEdge(
                    id=f"join_{i}",
                    nodes=edge_nodes,
                    label=f"{join_type} Join"
                ))

        return edges

    def _calculate_complexity_metrics(
        self,
        nodes: list[HypergraphNode],
        edges: list[HypergraphEdge],
        tables: set,
        join_predicates: list,
        aggregates: list,
        is_acyclic: bool,
        join_tree: list,
        gyo_steps: list
    ) -> dict:
        """
        Calculate various complexity metrics for the query

        Returns a dictionary with the following metrics:
        - num_tables: Number of tables/relations
        - num_joins: Number of join predicates
        - num_attributes: Total number of unique attributes
        - num_output_attributes: Number of attributes in SELECT clause
        - num_predicates: Total predicates (joins + filters)
        - max_node_degree: Maximum degree of any attribute node
        - max_edge_size: Maximum size of any hyperedge
        - avg_edge_size: Average size of hyperedges
        - hypergraph_density: Density measure
        - join_tree_depth: Depth of join tree (if acyclic)
        """
        metrics = {}

        # Basic counts
        metrics['num_tables'] = len(tables)
        metrics['num_joins'] = len(join_predicates)
        metrics['num_aggregates'] = len(aggregates)

        # Attribute counts
        all_attributes = set()
        output_attributes = set()
        for node in nodes:
            all_attributes.update(node.attributes)
            output_attributes.update(node.output_attributes)

        metrics['num_attributes'] = len(all_attributes)
        metrics['num_output_attributes'] = len(output_attributes)

        # Hypergraph structure metrics
        if edges:
            edge_sizes = [len(edge.nodes) for edge in edges]
            metrics['max_edge_size'] = max(edge_sizes) if edge_sizes else 0
            metrics['avg_edge_size'] = round(sum(edge_sizes) / len(edge_sizes), 2) if edge_sizes else 0
        else:
            metrics['max_edge_size'] = 0
            metrics['avg_edge_size'] = 0

        # Node degree calculation
        node_degrees = {}
        for edge in edges:
            for node_id in edge.nodes:
                node_degrees[node_id] = node_degrees.get(node_id, 0) + 1

        metrics['max_node_degree'] = max(node_degrees.values()) if node_degrees else 0
        metrics['avg_node_degree'] = round(sum(node_degrees.values()) / len(node_degrees), 2) if node_degrees else 0

        # Hypergraph density (edges / possible_edges)
        # For hypergraphs, this is a rough approximation
        num_nodes = len(nodes)
        num_edges = len(edges)
        if num_nodes > 1:
            # Maximum possible hyperedges with n nodes
            max_possible_edges = 2**num_nodes - num_nodes - 1  # All subsets of size >= 2
            metrics['hypergraph_density'] = round(num_edges / max(max_possible_edges, 1), 4)
        else:
            metrics['hypergraph_density'] = 0.0

        # Acyclic-specific metrics
        if is_acyclic and join_tree:
            # Join tree depth
            max_level = max([node.level for node in join_tree]) if join_tree else 0
            metrics['join_tree_depth'] = max_level + 1
        else:
            metrics['join_tree_depth'] = None

        return metrics

    def _check_acyclic(
        self,
        nodes: list[HypergraphNode],
        edges: list[HypergraphEdge]
    ) -> bool:
        """
        Check if hypergraph is acyclic

        This is a simplified check. Proper acyclicity testing for hypergraphs
        is more complex and should ideally be done in the Scala code.
        """
        # Simplified: if we have n relations and n-1 joins, likely acyclic
        return len(edges) == len(nodes) - 1 if len(nodes) > 0 else True
