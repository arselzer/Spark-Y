"""
Collect operator-level metrics from Spark SQL execution
"""
from typing import Dict, List, Any, Optional
import logging
import requests

logger = logging.getLogger(__name__)


class OperatorMetricsCollector:
    """Collects operator-level metrics from Spark SQL execution via REST API"""

    def __init__(self, spark):
        self.spark = spark
        self.app_id = None
        self.api_url = None

        try:
            # Get application ID
            self.app_id = spark.sparkContext.applicationId

            # Get UI URL and construct API endpoint
            ui_web_url = spark.sparkContext.uiWebUrl
            if ui_web_url:
                from urllib.parse import urlsplit, urlunsplit
                parsed = list(urlsplit(ui_web_url))
                # Replace hostname with localhost but keep port
                host_port = parsed[1]
                if ':' in host_port:
                    port = host_port[host_port.find(':'):]
                    parsed[1] = 'localhost' + port
                else:
                    parsed[1] = 'localhost:4040'
                self.api_url = f'{urlunsplit(parsed)}/api/v1'
                logger.info(f"Operator metrics collector initialized: {self.api_url}")
        except Exception as e:
            logger.warning(f"Could not initialize operator metrics collector: {e}")

    def get_execution_id_by_description(self, description: str) -> Optional[int]:
        """
        Get SQL execution ID by matching description (job group ID)

        Args:
            description: The job group description to match

        Returns:
            The matching execution ID, or None if not found
        """
        if not self.api_url or not self.app_id:
            logger.warning("Cannot get SQL execution ID: missing API info")
            logger.warning(f"api_url={self.api_url}, app_id={self.app_id}")
            return None

        try:
            # Get list of all SQL executions with large limit to ensure we find it
            url = f"{self.api_url}/applications/{self.app_id}/sql"
            params = {'length': '100000'}  # Get a large number of queries
            logger.info(f"Querying SQL executions from: {url} with params: {params}")
            response = requests.get(url, params=params, timeout=5)

            if response.status_code != 200:
                logger.warning(f"Failed to get SQL executions: HTTP {response.status_code}")
                logger.warning(f"Response: {response.text[:200]}")
                return None

            executions = response.json()
            logger.info(f"Found {len(executions) if executions else 0} SQL executions total")

            # Find execution matching the description (job group ID)
            matching = [q for q in executions if q.get('description') == description]

            if matching:
                execution_id = matching[0].get('id')
                logger.info(f"Found matching SQL execution ID: {execution_id} for description: {description[:100]}")
                return execution_id
            else:
                logger.warning(f"No SQL execution found matching description: {description[:100]}")
                # Log available descriptions for debugging
                if executions:
                    recent_descs = [q.get('description', 'N/A')[:50] for q in executions[:5]]
                    logger.debug(f"Recent descriptions: {recent_descs}")
                return None

        except requests.exceptions.RequestException as e:
            logger.warning(f"Network error getting SQL execution ID: {e}")
            logger.warning(f"URL was: {url}")
            return None
        except Exception as e:
            logger.warning(f"Error getting SQL execution ID by description: {e}", exc_info=True)
            return None

    def collect_operator_metrics_from_plan(self, plan_tree: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Collect operator-level metrics from a physical plan tree

        Args:
            plan_tree: The parsed physical plan JSON tree

        Returns:
            List of operator nodes with metrics
        """
        if not plan_tree:
            logger.warning("Cannot collect operator metrics: plan_tree is None")
            return []

        try:
            # Extract operators from the plan tree
            operators = self._parse_plan_tree(plan_tree)

            if len(operators) == 0:
                logger.warning("No operators found in plan tree")
                logger.debug(f"Plan tree keys: {list(plan_tree.keys())[:5]}")
            else:
                logger.info(f"Collected {len(operators)} operators from plan tree")
                # Log first operator as sample
                if operators:
                    logger.debug(f"Sample operator: {operators[0]}")

            return operators

        except Exception as e:
            logger.warning(f"Error collecting operator metrics from plan: {e}", exc_info=True)
            return []

    def _parse_plan_tree(self, node: Dict[str, Any], operators: Optional[List[Dict[str, Any]]] = None, node_id_counter: Optional[List[int]] = None) -> List[Dict[str, Any]]:
        """
        Recursively parse the physical plan tree to extract operator nodes

        Args:
            node: Current node in the plan tree
            operators: Accumulator list for operators
            node_id_counter: Counter for generating unique node IDs

        Returns:
            List of operators found in the tree
        """
        if operators is None:
            operators = []
        if node_id_counter is None:
            node_id_counter = [0]  # Use list to allow mutation in nested calls

        try:
            # Assign unique ID to this node
            current_id = node_id_counter[0]
            node_id_counter[0] += 1

            # Extract operator info from this node
            operator = self._extract_operator_info_from_tree(node, current_id)
            if operator:
                operators.append(operator)

                # Recursively process children and collect their IDs
                children = node.get('children', [])
                if children:
                    child_ids = []
                    for child in children:
                        # The next node will get the next ID
                        child_id = node_id_counter[0]
                        child_ids.append(child_id)
                        self._parse_plan_tree(child, operators, node_id_counter)

                    # Update the operator's children list with actual child IDs
                    operator['children'] = child_ids

        except Exception as e:
            logger.debug(f"Error parsing plan tree node: {e}")

        return operators

    def _extract_operator_info_from_tree(self, node: Dict, node_id: int) -> Optional[Dict[str, Any]]:
        """Extract operator information from a plan tree node

        Args:
            node: The plan tree node
            node_id: Unique numeric ID for this operator instance

        Returns:
            Operator dictionary with unique ID
        """
        try:
            # Get the operator class name (e.g., "org.apache.spark.sql.execution.WholeStageCodegenExec")
            class_name = node.get('class', node.get('nodeName', ''))
            simple_name = class_name.split('.')[-1] if class_name else 'Unknown'

            # Try to extract number of output rows from metrics
            num_output_rows = 0
            metrics_dict = {}

            # Check if node has metrics with number of output rows
            if 'metrics' in node and isinstance(node['metrics'], list):
                for metric in node['metrics']:
                    if isinstance(metric, dict):
                        metric_name = metric.get('name', '')
                        metric_value = metric.get('value', 0)

                        # Store all metrics
                        metrics_dict[metric_name] = metric_value

                        # Look for output row count metric (check multiple possible names)
                        if metric_name in ['numOutputRows', 'number of output rows', 'outputRows']:
                            try:
                                num_output_rows = int(metric_value)
                            except (ValueError, TypeError):
                                pass

                        # For exchange/shuffle operators, also check for shuffle records
                        # These operators track "records written" instead of "output rows"
                        if metric_name in ['shuffle records written', 'records written', 'shuffleRecordsWritten']:
                            try:
                                num_output_rows = int(metric_value)
                            except (ValueError, TypeError):
                                pass

            operator = {
                'operator_id': node_id,  # Use unique numeric ID
                'operator_name': simple_name,  # Keep class name for display
                'operator_type': self._get_operator_type_from_name(simple_name),
                'num_output_rows': num_output_rows,
                'children': [],  # Will be filled in by _parse_plan_tree
                'metrics': metrics_dict
            }

            # Extract simpleString which contains the operator description
            simple_string = node.get('simpleString', '')
            if simple_string:
                operator['metrics']['simpleString'] = simple_string

            return operator

        except Exception as e:
            logger.debug(f"Error extracting operator info from tree: {e}")
            return None

    def _get_operator_type_from_name(self, name: str) -> str:
        """Determine operator type from operator name"""
        name_lower = name.lower()

        if 'scan' in name_lower:
            return 'scan'
        elif 'join' in name_lower or 'cartesian' in name_lower:
            return 'join'
        elif 'aggregate' in name_lower or 'agg' in name_lower:
            return 'aggregate'
        elif 'exchange' in name_lower or 'shuffle' in name_lower:
            return 'exchange'
        elif 'sort' in name_lower:
            return 'sort'
        elif 'filter' in name_lower:
            return 'filter'
        elif 'project' in name_lower:
            return 'project'
        else:
            return 'other'

    def _get_operator_type(self, node: Dict) -> str:
        """Determine operator type from node (legacy method)"""
        name = node.get('name', node.get('nodeName', ''))
        return self._get_operator_type_from_name(name)
