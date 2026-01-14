"""
Custom metrics collector for detailed Spark execution analysis
"""
from typing import Dict, List, Any, Optional
import logging
import re

logger = logging.getLogger(__name__)


class MetricsCollector:
    """Collects detailed execution metrics from Spark query execution"""

    def __init__(self):
        self.reset()

    def reset(self):
        """Reset metrics for new query execution"""
        self.operator_counts: Dict[str, int] = {}
        self.shuffle_read_bytes = 0
        self.shuffle_write_bytes = 0
        self.input_bytes = 0
        self.output_bytes = 0
        self.peak_memory_bytes = 0
        self.total_task_time_ms = 0
        self.num_tasks = 0
        self.stage_count = 0
        self.stages: List[Dict[str, Any]] = []

    def collect_from_query_execution(self, query_execution) -> Dict[str, Any]:
        """
        Extract metrics from completed query execution

        Args:
            query_execution: Spark query execution object

        Returns:
            Dictionary of collected metrics
        """
        try:
            # Get executed plan
            executed_plan = query_execution.executedPlan()
            physical_plan_str = executed_plan.toString()

            # Try to get JSON representation for more accurate parsing
            try:
                physical_plan_json_str = executed_plan.toJSON()
                physical_plan_json = self._parse_plan_json(physical_plan_json_str)
                # Count operators from JSON (more accurate)
                self.operator_counts = self._count_operators_from_json(physical_plan_json)
                logger.debug(f"Using JSON-based operator counting: {self.operator_counts}")
            except Exception as e:
                logger.debug(f"Could not parse plan JSON, falling back to string parsing: {e}")
                # Fallback to string-based counting
                self.operator_counts = self._count_operators(physical_plan_str)

            # Extract stage count (simple count of Exchange operators)
            self.stage_count = self._count_stages(physical_plan_str)
            # Stage details come from real metrics collection (REST API), not from plan parsing
            self.stages = []

            # Try to extract metrics from plan annotations
            self._extract_plan_annotations(physical_plan_str)

            return self.get_metrics_dict()

        except Exception as e:
            logger.warning(f"Error collecting metrics: {e}", exc_info=True)
            return self.get_metrics_dict()

    def _parse_plan_json(self, json_str: str) -> Dict[str, Any]:
        """Parse JSON string to dict"""
        import json
        # The JSON might be a list or a dict
        parsed = json.loads(json_str)
        if isinstance(parsed, list):
            return parsed[0] if parsed else {}
        return parsed

    def _count_operators_from_json(self, plan_node: Any) -> Dict[str, int]:
        """
        Count operators by recursively traversing the JSON plan tree

        Args:
            plan_node: Dict or List representing a plan node

        Returns:
            Dictionary of operator counts
        """
        operators = {
            'scans': 0,
            'joins': 0,
            'aggregates': 0,
            'exchanges': 0,
            'sorts': 0,
            'projects': 0,
            'filters': 0
        }

        def traverse(node):
            """Recursively traverse plan tree and count operators"""
            if node is None:
                return

            # Handle both list and dict formats
            if isinstance(node, list):
                for item in node:
                    traverse(item)
                return

            if not isinstance(node, dict):
                return

            # Get node class/type
            node_class = node.get('class', '') or node.get('nodeName', '') or node.get('simpleString', '')

            # Count based on operator type
            if 'Scan' in node_class:
                operators['scans'] += 1
            elif any(jt in node_class for jt in ['BroadcastHashJoin', 'SortMergeJoin', 'ShuffledHashJoin',
                                                   'BroadcastNestedLoopJoin', 'CartesianProduct']):
                operators['joins'] += 1
            elif any(at in node_class for at in ['HashAggregate', 'ObjectHashAggregate', 'SortAggregate']):
                operators['aggregates'] += 1
            elif 'Exchange' in node_class:
                operators['exchanges'] += 1
            elif 'Sort' in node_class and 'Merge' not in node_class:  # Exclude SortMergeJoin
                operators['sorts'] += 1
            elif 'Project' in node_class:
                operators['projects'] += 1
            elif 'Filter' in node_class:
                operators['filters'] += 1

            # Traverse children
            if 'children' in node:
                for child in node['children']:
                    traverse(child)

            # Some JSON formats use 'left' and 'right' for join children
            if 'left' in node:
                traverse(node['left'])
            if 'right' in node:
                traverse(node['right'])

        traverse(plan_node)
        return operators

    def _count_operators(self, plan_str: str) -> Dict[str, int]:
        """Count operators in physical plan"""
        operators = {
            'scans': 0,
            'joins': 0,
            'aggregates': 0,
            'exchanges': 0,
            'sorts': 0,
            'projects': 0,
            'filters': 0
        }

        # Count each operator type
        operators['scans'] = plan_str.count('Scan')

        # Count joins - each type individually to avoid double counting
        operators['joins'] = plan_str.count('BroadcastHashJoin')
        operators['joins'] += plan_str.count('SortMergeJoin')
        operators['joins'] += plan_str.count('ShuffledHashJoin')
        operators['joins'] += plan_str.count('BroadcastNestedLoopJoin')
        operators['joins'] += plan_str.count('CartesianProduct')

        # Count aggregates
        operators['aggregates'] = plan_str.count('HashAggregate')
        operators['aggregates'] += plan_str.count('ObjectHashAggregate')
        operators['aggregates'] += plan_str.count('SortAggregate')

        operators['exchanges'] = plan_str.count('Exchange')
        operators['sorts'] = plan_str.count('Sort')
        operators['projects'] = plan_str.count('Project')
        operators['filters'] = plan_str.count('Filter')

        return operators

    def _count_stages(self, plan_str: str) -> int:
        """
        Estimate number of stages based on Exchange operators
        Each Exchange typically represents a shuffle boundary (new stage)
        """
        # Count Exchange operators (shuffle boundaries)
        exchanges = plan_str.count('Exchange')
        # Stages = exchanges + 1 (at least one stage even with no shuffles)
        return max(1, exchanges + 1)

    def _extract_plan_annotations(self, plan_str: str):
        """Extract metrics from plan annotations if available"""
        # Try to extract Statistics annotations
        # Spark uses both decimal (KB, MB, GB) and binary (KiB, MiB, GiB, EiB) units
        # Example: Statistics(sizeInBytes=1.2 GB, rowCount=1.0E7)
        # Example: Statistics(sizeInBytes=16.0 MiB)
        # Example: Statistics(sizeInBytes=8.0 EiB, rowCount=1.00E+8)

        # Updated pattern to match both decimal and binary units, with optional 'i'
        stats_pattern = r'Statistics\(sizeInBytes=([\d.E+-]+)\s*([KMGTPE]i?B)'
        matches = re.findall(stats_pattern, plan_str)

        if matches:
            # Sum up all size annotations as estimate
            total_bytes = 0
            for size_str, unit in matches:
                size = float(size_str)  # Handles scientific notation like 1.2E+3

                # Binary units (base 1024)
                multiplier = {
                    'B': 1,
                    'KiB': 1024,
                    'MiB': 1024**2,
                    'GiB': 1024**3,
                    'TiB': 1024**4,
                    'PiB': 1024**5,
                    'EiB': 1024**6,
                    # Decimal units (base 1000) - less common but possible
                    'KB': 1000,
                    'MB': 1000**2,
                    'GB': 1000**3,
                    'TB': 1000**4,
                    'PB': 1000**5,
                    'EB': 1000**6
                }
                total_bytes += size * multiplier.get(unit, 1)

            self.input_bytes = int(total_bytes)

    def get_metrics_dict(self) -> Dict[str, Any]:
        """Return collected metrics as dictionary"""
        return {
            'operator_counts': self.operator_counts,
            'shuffle_read_bytes': self.shuffle_read_bytes,
            'shuffle_write_bytes': self.shuffle_write_bytes,
            'input_bytes': self.input_bytes,
            'output_bytes': self.output_bytes,
            'peak_memory_bytes': self.peak_memory_bytes,
            'total_task_time_ms': self.total_task_time_ms,
            'num_tasks': self.num_tasks,
            'stage_count': self.stage_count,
            'stages': self.stages
        }
