"""
Convert Spark SQL logical plans to JSON representation

This module provides utilities to convert Spark's internal LogicalPlan
tree structure into JSON format, which can then be used for hypergraph extraction.
"""
import json
import logging
from typing import Dict, List, Any, Optional
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class LogicalPlanJsonConverter:
    """Converts Spark LogicalPlan to JSON representation"""

    def __init__(self, spark_session):
        self.spark = spark_session
        self.jvm = spark_session.sparkContext._jvm

    def get_logical_plan_json(self, df: DataFrame) -> Dict[str, Any]:
        """
        Extract logical plan from DataFrame and convert to JSON

        Args:
            df: Spark DataFrame

        Returns:
            Dict containing the logical plan structure
        """
        try:
            # Get the logical plan
            logical_plan = df._jdf.queryExecution().logical()

            # Convert to JSON structure by traversing the plan tree
            plan_json = self._traverse_plan(logical_plan)

            return plan_json

        except Exception as e:
            logger.error(f"Failed to extract logical plan as JSON: {e}")
            raise

    def get_analyzed_plan_json(self, df: DataFrame) -> Dict[str, Any]:
        """
        Get analyzed (resolved) logical plan as JSON

        The analyzed plan has all tables/columns resolved and types inferred.
        This is generally more useful than the raw logical plan.
        """
        try:
            analyzed_plan = df._jdf.queryExecution().analyzed()
            return self._traverse_plan(analyzed_plan)
        except Exception as e:
            logger.error(f"Failed to extract analyzed plan as JSON: {e}")
            raise

    def get_optimized_plan_json(self, df: DataFrame) -> Dict[str, Any]:
        """
        Get optimized logical plan as JSON

        This plan has undergone Catalyst optimizations.
        """
        try:
            optimized_plan = df._jdf.queryExecution().optimizedPlan()
            return self._traverse_plan(optimized_plan)
        except Exception as e:
            logger.error(f"Failed to extract optimized plan as JSON: {e}")
            raise

    def _traverse_plan(self, plan_node) -> Dict[str, Any]:
        """
        Recursively traverse a LogicalPlan tree node and convert to JSON

        Args:
            plan_node: A Spark LogicalPlan node (JVM object)

        Returns:
            Dict representation of the plan node
        """
        try:
            # Get node class name
            node_class = plan_node.getClass().getSimpleName()

            # Build JSON structure
            node_json = {
                "nodeType": node_class,
                "nodeName": self._get_node_name(plan_node, node_class),
                "children": [],
                "expressions": [],
                "metadata": {}
            }

            # Extract metadata based on node type
            node_json["metadata"] = self._extract_node_metadata(plan_node, node_class)

            # Get children (recursive)
            try:
                # Spark's children() returns a Scala Seq
                # We need to convert it properly using Py4J
                children_seq = plan_node.children()
                children = []

                logger.info(f"Converting children for node {node_class}, type: {type(children_seq)}")

                # Try multiple conversion strategies
                # Strategy 1: Use size() and apply() - most reliable method
                try:
                    if hasattr(children_seq, 'size'):
                        logger.info("Trying size() + apply() method")
                        size = children_seq.size()
                        children = [children_seq.apply(i) for i in range(size)]
                        logger.info(f"size()+apply() succeeded, got {len(children)} children")
                    else:
                        logger.warning("children_seq does not have size() method")
                except Exception as e1:
                    logger.warning(f"size()+apply() failed: {e1}")

                    # Strategy 2: Try Python iteration
                    try:
                        logger.info("Trying Python iteration")
                        for child in children_seq:
                            children.append(child)
                        logger.info(f"Python iteration succeeded, got {len(children)} children")
                    except Exception as e2:
                        logger.error(f"All conversion strategies failed: size+apply={e1}, iteration={e2}")

                logger.info(f"Node {node_class} has {len(children)} children")

                for child in children:
                    child_json = self._traverse_plan(child)
                    node_json["children"].append(child_json)
            except Exception as e:
                logger.error(f"Fatal error getting children for {node_class}: {e}", exc_info=True)

            # Extract expressions if available
            try:
                # Try to get output attributes (also a Scala Seq)
                output_seq = plan_node.output()

                # Convert Scala Seq to Python list - use size()+apply() method
                if hasattr(output_seq, 'size'):
                    size = output_seq.size()
                    output = [output_seq.apply(i) for i in range(size)]
                else:
                    output = []
                    for attr in output_seq:
                        output.append(attr)

                node_json["output"] = [self._attribute_to_json(attr) for attr in output]
            except Exception as e:
                logger.debug(f"No output or error getting output for {node_class}: {e}")

            return node_json

        except Exception as e:
            logger.warning(f"Error traversing plan node: {e}")
            return {
                "nodeType": "Unknown",
                "error": str(e),
                "children": []
            }

    def _get_node_name(self, plan_node, node_class: str) -> str:
        """Get a human-readable name for the plan node"""
        try:
            # For Relation nodes, try to get table name
            if node_class == "LogicalRelation" or node_class == "HiveTableRelation":
                try:
                    # Try to get table identifier
                    relation = plan_node.relation()
                    return f"Relation({relation})"
                except:
                    pass

            # For SubqueryAlias, get the alias name
            if node_class == "SubqueryAlias":
                try:
                    alias = plan_node.alias()
                    return f"Table: {alias}"
                except:
                    pass

            # For Join nodes, get join type
            if node_class == "Join":
                try:
                    join_type = str(plan_node.joinType())
                    return f"Join({join_type})"
                except:
                    pass

            # For Aggregate, show grouping
            if node_class == "Aggregate":
                return "Aggregate"

            # For Filter, show it's a filter
            if node_class == "Filter":
                return "Filter"

            # Default: use class name
            return node_class

        except Exception as e:
            logger.debug(f"Error getting node name: {e}")
            return node_class

    def _extract_node_metadata(self, plan_node, node_class: str) -> Dict[str, Any]:
        """Extract metadata from specific node types"""
        metadata = {}

        try:
            # Extract metadata based on node type
            if node_class == "SubqueryAlias":
                try:
                    metadata["alias"] = str(plan_node.alias())
                except:
                    pass

            elif node_class == "Join":
                try:
                    metadata["joinType"] = str(plan_node.joinType())
                    # Try to get join condition
                    try:
                        condition = plan_node.condition()
                        if condition.isDefined():
                            metadata["condition"] = str(condition.get())
                    except:
                        pass
                except:
                    pass

            elif node_class == "Filter":
                try:
                    condition = plan_node.condition()
                    metadata["condition"] = str(condition)
                except:
                    pass

            elif node_class == "Aggregate":
                logger.info("Extracting Aggregate metadata")
                try:
                    # Get grouping expressions (Scala Seq)
                    grouping_seq = plan_node.groupingExpressions()
                    logger.info(f"Got groupingExpressions, type: {type(grouping_seq)}")

                    grouping = []
                    if hasattr(grouping_seq, 'size'):
                        logger.info("Trying size()+apply() for grouping")
                        size = grouping_seq.size()
                        grouping = [grouping_seq.apply(i) for i in range(size)]
                    else:
                        logger.warning("Using direct list() conversion for grouping")
                        grouping = list(grouping_seq)

                    metadata["groupBy"] = [str(expr) for expr in grouping]
                    logger.info(f"Extracted {len(grouping)} grouping expressions")

                    # Get aggregate expressions (Scala Seq)
                    agg_seq = plan_node.aggregateExpressions()
                    logger.info(f"Got aggregateExpressions, type: {type(agg_seq)}")

                    agg_exprs = []
                    if hasattr(agg_seq, 'size'):
                        logger.info("Trying size()+apply() for aggregates")
                        size = agg_seq.size()
                        agg_exprs = [agg_seq.apply(i) for i in range(size)]
                    else:
                        logger.warning("Using direct list() conversion for aggregates")
                        agg_exprs = list(agg_seq)

                    metadata["aggregates"] = [str(expr) for expr in agg_exprs]
                    logger.info(f"Extracted {len(agg_exprs)} aggregate expressions")
                except Exception as e:
                    logger.error(f"Error extracting Aggregate metadata: {e}", exc_info=True)

            elif node_class == "Project":
                try:
                    # Get project list (Scala Seq)
                    proj_seq = plan_node.projectList()
                    if hasattr(proj_seq, 'size'):
                        size = proj_seq.size()
                        projectList = [proj_seq.apply(i) for i in range(size)]
                    else:
                        projectList = list(proj_seq)
                    metadata["projections"] = [str(proj) for proj in projectList]
                except Exception as e:
                    logger.debug(f"Error extracting Project metadata: {e}")

        except Exception as e:
            logger.debug(f"Error extracting metadata: {e}")

        return metadata

    def _attribute_to_json(self, attribute) -> Dict[str, Any]:
        """Convert an Attribute to JSON"""
        try:
            return {
                "name": str(attribute.name()),
                "dataType": str(attribute.dataType()),
                "qualifier": str(attribute.qualifier()) if hasattr(attribute, 'qualifier') else None,
                "exprId": str(attribute.exprId()) if hasattr(attribute, 'exprId') else None
            }
        except Exception as e:
            return {"name": str(attribute), "error": str(e)}


def get_plan_as_json(df: DataFrame, plan_type: str = "analyzed") -> Dict[str, Any]:
    """
    Convenience function to get a DataFrame's query plan as JSON

    Args:
        df: Spark DataFrame
        plan_type: One of "logical", "analyzed", or "optimized"

    Returns:
        Dict containing the plan structure

    Example:
        >>> df = spark.sql("SELECT * FROM table1 JOIN table2 ON table1.id = table2.id")
        >>> plan_json = get_plan_as_json(df, "analyzed")
        >>> print(json.dumps(plan_json, indent=2))
    """
    converter = LogicalPlanJsonConverter(df.sparkSession)

    if plan_type == "logical":
        return converter.get_logical_plan_json(df)
    elif plan_type == "analyzed":
        return converter.get_analyzed_plan_json(df)
    elif plan_type == "optimized":
        return converter.get_optimized_plan_json(df)
    else:
        raise ValueError(f"Unknown plan_type: {plan_type}. Use 'logical', 'analyzed', or 'optimized'")
