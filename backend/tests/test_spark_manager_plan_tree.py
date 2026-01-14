"""
Tests for SparkManager plan tree building functionality
"""
import pytest
from unittest.mock import Mock, MagicMock
from app.spark.spark_manager import SparkManager


def create_mock_scala_seq(items):
    """Create a mock Scala Seq that behaves like Spark's Seq[SparkPlan]"""
    mock_seq = Mock()
    mock_seq.size.return_value = len(items)
    mock_seq.apply.side_effect = lambda i: items[i]
    return mock_seq


class TestPlanTreeBuilding:
    """Test the _build_plan_tree_from_operator method"""

    @pytest.fixture
    def spark_manager(self):
        """Create a SparkManager instance for testing"""
        manager = SparkManager()
        return manager

    def test_build_plan_tree_single_operator(self, spark_manager):
        """Test building tree from a single operator with no children"""
        # Mock a Spark operator
        mock_operator = Mock()
        mock_operator.getClass().getName.return_value = "org.apache.spark.sql.execution.FileSourceScanExec"
        # simpleString is a property, not a method - just assign directly
        mock_operator.simpleString = "FileScan parquet [id, name]"
        mock_operator.children.return_value = create_mock_scala_seq([])  # No children

        result = spark_manager._build_plan_tree_from_operator(mock_operator)

        assert result is not None
        assert result['class'] == "org.apache.spark.sql.execution.FileSourceScanExec"
        assert "FileScan parquet" in result['simpleString']
        assert result['children'] == []

    def test_build_plan_tree_with_children(self, spark_manager):
        """Test building tree from operator with children"""
        # Mock child operator
        mock_child = Mock()
        mock_child.getClass().getName.return_value = "org.apache.spark.sql.execution.FileSourceScanExec"
        mock_child.simpleString = "FileScan parquet [id]"
        mock_child.children.return_value = create_mock_scala_seq([])

        # Mock parent operator
        mock_parent = Mock()
        mock_parent.getClass().getName.return_value = "org.apache.spark.sql.execution.FilterExec"
        mock_parent.simpleString = "Filter (id > 10)"
        mock_parent.children.return_value = create_mock_scala_seq([mock_child])

        result = spark_manager._build_plan_tree_from_operator(mock_parent)

        assert result is not None
        assert result['class'] == "org.apache.spark.sql.execution.FilterExec"
        assert len(result['children']) == 1
        assert result['children'][0]['class'] == "org.apache.spark.sql.execution.FileSourceScanExec"

    def test_build_plan_tree_nested_hierarchy(self, spark_manager):
        """Test building tree with multiple levels of nesting"""
        # Mock leaf operator (deepest level)
        mock_scan = Mock()
        mock_scan.getClass().getName.return_value = "org.apache.spark.sql.execution.FileSourceScanExec"
        mock_scan.simpleString = "FileScan parquet [id]"
        mock_scan.children.return_value = create_mock_scala_seq([])

        # Mock middle operator
        mock_project = Mock()
        mock_project.getClass().getName.return_value = "org.apache.spark.sql.execution.ProjectExec"
        mock_project.simpleString = "Project [id, name]"
        mock_project.children.return_value = create_mock_scala_seq([mock_scan])

        # Mock root operator
        mock_aggregate = Mock()
        mock_aggregate.getClass().getName.return_value = "org.apache.spark.sql.execution.aggregate.HashAggregateExec"
        mock_aggregate.simpleString = "HashAggregate(keys=[], functions=[sum(id)])"
        mock_aggregate.children.return_value = create_mock_scala_seq([mock_project])

        result = spark_manager._build_plan_tree_from_operator(mock_aggregate)

        assert result is not None
        assert result['class'] == "org.apache.spark.sql.execution.aggregate.HashAggregateExec"
        assert len(result['children']) == 1

        # Check middle level
        project_node = result['children'][0]
        assert project_node['class'] == "org.apache.spark.sql.execution.ProjectExec"
        assert len(project_node['children']) == 1

        # Check leaf level
        scan_node = project_node['children'][0]
        assert scan_node['class'] == "org.apache.spark.sql.execution.FileSourceScanExec"
        assert scan_node['children'] == []

    def test_build_plan_tree_multiple_children(self, spark_manager):
        """Test building tree from operator with multiple children (like a join)"""
        # Mock left child
        mock_left = Mock()
        mock_left.getClass().getName.return_value = "org.apache.spark.sql.execution.FileSourceScanExec"
        mock_left.simpleString = "FileScan parquet orders"
        mock_left.children.return_value = create_mock_scala_seq([])

        # Mock right child
        mock_right = Mock()
        mock_right.getClass().getName.return_value = "org.apache.spark.sql.execution.FileSourceScanExec"
        mock_right.simpleString = "FileScan parquet customers"
        mock_right.children.return_value = create_mock_scala_seq([])

        # Mock join operator
        mock_join = Mock()
        mock_join.getClass().getName.return_value = "org.apache.spark.sql.execution.joins.SortMergeJoinExec"
        mock_join.simpleString = "SortMergeJoin [id]"
        mock_join.children.return_value = create_mock_scala_seq([mock_left, mock_right])

        result = spark_manager._build_plan_tree_from_operator(mock_join)

        assert result is not None
        assert result['class'] == "org.apache.spark.sql.execution.joins.SortMergeJoinExec"
        assert len(result['children']) == 2
        assert result['children'][0]['class'] == "org.apache.spark.sql.execution.FileSourceScanExec"
        assert result['children'][1]['class'] == "org.apache.spark.sql.execution.FileSourceScanExec"
        assert "orders" in result['children'][0]['simpleString']
        assert "customers" in result['children'][1]['simpleString']

    def test_build_plan_tree_error_handling(self, spark_manager):
        """Test that errors are handled gracefully"""
        # Mock operator that raises exception
        mock_operator = Mock()
        mock_operator.getClass.side_effect = Exception("Test error")

        result = spark_manager._build_plan_tree_from_operator(mock_operator)

        # Should return None on error, not crash
        assert result is None

    def test_build_plan_tree_no_children_method(self, spark_manager):
        """Test operator without children() method"""
        # Mock operator without children method
        mock_operator = Mock()
        mock_operator.getClass().getName.return_value = "org.apache.spark.sql.execution.SomeExec"
        mock_operator.simpleString = "Some operator"
        # Remove children method
        del mock_operator.children

        result = spark_manager._build_plan_tree_from_operator(mock_operator)

        # Should still create node, just with empty children
        assert result is not None
        assert result['class'] == "org.apache.spark.sql.execution.SomeExec"
        assert result['children'] == []

    def test_build_plan_tree_complex_hierarchy(self, spark_manager):
        """Test a complex query plan with multiple levels and branches"""
        # Build a tree like:
        #   HashAggregate
        #     -> Exchange
        #          -> HashAggregate
        #               -> Project
        #                    -> Join
        #                         -> Scan (left)
        #                         -> Scan (right)

        # Leaf scans
        scan_left = Mock()
        scan_left.getClass().getName.return_value = "org.apache.spark.sql.execution.FileSourceScanExec"
        scan_left.simpleString = "FileScan parquet orders"
        scan_left.children.return_value = create_mock_scala_seq([])

        scan_right = Mock()
        scan_right.getClass().getName.return_value = "org.apache.spark.sql.execution.FileSourceScanExec"
        scan_right.simpleString = "FileScan parquet customers"
        scan_right.children.return_value = create_mock_scala_seq([])

        # Join
        join = Mock()
        join.getClass().getName.return_value = "org.apache.spark.sql.execution.joins.BroadcastHashJoinExec"
        join.simpleString = "BroadcastHashJoin [id]"
        join.children.return_value = create_mock_scala_seq([scan_left, scan_right])

        # Project
        project = Mock()
        project.getClass().getName.return_value = "org.apache.spark.sql.execution.ProjectExec"
        project.simpleString = "Project [revenue]"
        project.children.return_value = create_mock_scala_seq([join])

        # Partial aggregate
        partial_agg = Mock()
        partial_agg.getClass().getName.return_value = "org.apache.spark.sql.execution.aggregate.HashAggregateExec"
        partial_agg.simpleString = "HashAggregate(partial_sum)"
        partial_agg.children.return_value = create_mock_scala_seq([project])

        # Exchange
        exchange = Mock()
        exchange.getClass().getName.return_value = "org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"
        exchange.simpleString = "Exchange SinglePartition"
        exchange.children.return_value = create_mock_scala_seq([partial_agg])

        # Final aggregate (root)
        final_agg = Mock()
        final_agg.getClass().getName.return_value = "org.apache.spark.sql.execution.aggregate.HashAggregateExec"
        final_agg.simpleString = "HashAggregate(sum)"
        final_agg.children.return_value = create_mock_scala_seq([exchange])

        result = spark_manager._build_plan_tree_from_operator(final_agg)

        # Verify structure
        assert result is not None
        assert "HashAggregate" in result['class']
        assert len(result['children']) == 1

        # Level 1: Exchange
        exchange_node = result['children'][0]
        assert "Exchange" in exchange_node['class']
        assert len(exchange_node['children']) == 1

        # Level 2: Partial Aggregate
        partial_agg_node = exchange_node['children'][0]
        assert "HashAggregate" in partial_agg_node['class']
        assert len(partial_agg_node['children']) == 1

        # Level 3: Project
        project_node = partial_agg_node['children'][0]
        assert "Project" in project_node['class']
        assert len(project_node['children']) == 1

        # Level 4: Join
        join_node = project_node['children'][0]
        assert "Join" in join_node['class']
        assert len(join_node['children']) == 2

        # Level 5: Scans
        assert "FileSourceScanExec" in join_node['children'][0]['class']
        assert "FileSourceScanExec" in join_node['children'][1]['class']
        assert join_node['children'][0]['children'] == []
        assert join_node['children'][1]['children'] == []


class TestPlanTreeIntegration:
    """Integration tests for plan tree building with operator metrics"""

    def test_plan_tree_structure_matches_expected_format(self):
        """Verify that plan tree format matches what operator metrics collector expects"""
        manager = SparkManager()

        # Mock a simple operator
        mock_operator = Mock()
        mock_operator.getClass().getName.return_value = "org.apache.spark.sql.execution.FileSourceScanExec"
        mock_operator.simpleString = "FileScan parquet [id]"
        mock_operator.children.return_value = create_mock_scala_seq([])

        plan_tree = manager._build_plan_tree_from_operator(mock_operator)

        # Verify it has the expected keys for OperatorMetricsCollector
        assert 'class' in plan_tree
        assert 'simpleString' in plan_tree
        assert 'children' in plan_tree

        # Verify children is a list
        assert isinstance(plan_tree['children'], list)
