"""
Tests for operator metrics collection from physical plan trees
"""
import pytest
from unittest.mock import Mock, MagicMock
from app.spark.operator_metrics_collector import OperatorMetricsCollector


class TestOperatorMetricsCollector:
    """Test OperatorMetricsCollector class"""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock Spark session"""
        spark = Mock()
        spark.sparkContext.applicationId = "test-app-123"
        spark.sparkContext.uiWebUrl = "http://spark-ui:4040"
        return spark

    @pytest.fixture
    def collector(self, mock_spark):
        """Create an OperatorMetricsCollector instance"""
        return OperatorMetricsCollector(mock_spark)

    def test_initialization(self, collector):
        """Test collector initialization"""
        assert collector.app_id == "test-app-123"
        assert collector.api_url == "http://localhost:4040/api/v1"

    def test_initialization_without_ui_url(self):
        """Test initialization when UI URL is not available"""
        spark = Mock()
        spark.sparkContext.applicationId = "test-app"
        spark.sparkContext.uiWebUrl = None

        collector = OperatorMetricsCollector(spark)
        assert collector.app_id == "test-app"
        assert collector.api_url is None

    def test_get_operator_type_from_name(self, collector):
        """Test operator type classification from name"""
        assert collector._get_operator_type_from_name("FileScan") == "scan"
        assert collector._get_operator_type_from_name("HashJoin") == "join"
        assert collector._get_operator_type_from_name("SortMergeJoin") == "join"
        assert collector._get_operator_type_from_name("HashAggregate") == "aggregate"
        assert collector._get_operator_type_from_name("Exchange") == "exchange"
        assert collector._get_operator_type_from_name("ShuffleExchange") == "exchange"
        assert collector._get_operator_type_from_name("Sort") == "sort"
        assert collector._get_operator_type_from_name("Filter") == "filter"
        assert collector._get_operator_type_from_name("Project") == "project"
        assert collector._get_operator_type_from_name("UnknownOperator") == "other"

    def test_extract_operator_info_from_tree_basic(self, collector):
        """Test extracting operator info from a simple node"""
        node = {
            "class": "org.apache.spark.sql.execution.FileSourceScanExec",
            "simpleString": "FileScan parquet [id, name]",
            "children": []
        }

        operator = collector._extract_operator_info_from_tree(node, node_id=0)

        assert operator is not None
        assert operator['operator_id'] == 0  # Now using numeric IDs
        assert operator['operator_name'] == "FileSourceScanExec"
        assert operator['operator_type'] == "scan"
        assert operator['num_output_rows'] == 0
        assert operator['children'] == []
        assert 'simpleString' in operator['metrics']

    def test_extract_operator_info_from_tree_with_children(self, collector):
        """Test extracting operator info with children"""
        node = {
            "class": "org.apache.spark.sql.execution.joins.SortMergeJoinExec",
            "simpleString": "SortMergeJoin [id]",
            "children": [
                {"class": "org.apache.spark.sql.execution.FileSourceScanExec"},
                {"class": "org.apache.spark.sql.execution.FileSourceScanExec"}
            ]
        }

        operator = collector._extract_operator_info_from_tree(node, node_id=0)

        assert operator is not None
        assert operator['operator_id'] == 0  # Now using numeric IDs
        assert operator['operator_type'] == "join"
        # Children list is empty initially, will be filled by _parse_plan_tree
        assert operator['children'] == []

    def test_parse_plan_tree_simple(self, collector):
        """Test parsing a simple plan tree"""
        plan_tree = {
            "class": "org.apache.spark.sql.execution.FileSourceScanExec",
            "simpleString": "FileScan parquet [id]",
            "children": []
        }

        operators = collector._parse_plan_tree(plan_tree)

        assert len(operators) == 1
        assert operators[0]['operator_type'] == "scan"

    def test_parse_plan_tree_nested(self, collector):
        """Test parsing a nested plan tree"""
        plan_tree = {
            "class": "org.apache.spark.sql.execution.aggregate.HashAggregateExec",
            "simpleString": "HashAggregate(keys=[id], functions=[sum(amount)])",
            "children": [
                {
                    "class": "org.apache.spark.sql.execution.exchange.ShuffleExchangeExec",
                    "simpleString": "Exchange hashpartitioning(id, 200)",
                    "children": [
                        {
                            "class": "org.apache.spark.sql.execution.FileSourceScanExec",
                            "simpleString": "FileScan parquet [id, amount]",
                            "children": []
                        }
                    ]
                }
            ]
        }

        operators = collector._parse_plan_tree(plan_tree)

        # Should have 3 operators: HashAggregate, Exchange, FileScan
        assert len(operators) == 3

        # Check operator types
        operator_types = [op['operator_type'] for op in operators]
        assert 'aggregate' in operator_types
        assert 'exchange' in operator_types
        assert 'scan' in operator_types

    def test_parse_plan_tree_multiple_children(self, collector):
        """Test parsing plan tree with multiple children (join)"""
        plan_tree = {
            "class": "org.apache.spark.sql.execution.joins.SortMergeJoinExec",
            "simpleString": "SortMergeJoin [id]",
            "children": [
                {
                    "class": "org.apache.spark.sql.execution.FileSourceScanExec",
                    "simpleString": "FileScan parquet table1",
                    "children": []
                },
                {
                    "class": "org.apache.spark.sql.execution.FileSourceScanExec",
                    "simpleString": "FileScan parquet table2",
                    "children": []
                }
            ]
        }

        operators = collector._parse_plan_tree(plan_tree)

        # Should have 3 operators: Join + 2 Scans
        assert len(operators) == 3

        # Find the join operator
        join_ops = [op for op in operators if op['operator_type'] == 'join']
        assert len(join_ops) == 1
        assert len(join_ops[0]['children']) == 2

    def test_collect_operator_metrics_from_plan(self, collector):
        """Test collect_operator_metrics_from_plan method"""
        plan_tree = {
            "class": "org.apache.spark.sql.execution.aggregate.HashAggregateExec",
            "simpleString": "HashAggregate",
            "children": [
                {
                    "class": "org.apache.spark.sql.execution.FileSourceScanExec",
                    "simpleString": "FileScan",
                    "children": []
                }
            ]
        }

        operators = collector.collect_operator_metrics_from_plan(plan_tree)

        assert len(operators) == 2
        assert operators[0]['operator_type'] == 'aggregate'
        assert operators[1]['operator_type'] == 'scan'

    def test_collect_operator_metrics_from_plan_none(self, collector):
        """Test collect_operator_metrics_from_plan with None input"""
        operators = collector.collect_operator_metrics_from_plan(None)
        assert operators == []

    def test_collect_operator_metrics_from_plan_empty_dict(self, collector):
        """Test collect_operator_metrics_from_plan with empty dict"""
        operators = collector.collect_operator_metrics_from_plan({})

        # Should handle gracefully and return empty or single operator
        assert isinstance(operators, list)

    def test_parse_plan_tree_malformed_node(self, collector):
        """Test parsing a malformed node"""
        # Node missing 'class' field
        node = {
            "simpleString": "Some operator",
            "children": []
        }

        operator = collector._extract_operator_info_from_tree(node, node_id=0)

        # Should handle gracefully
        assert operator is not None
        assert operator['operator_id'] == 0  # Now using numeric IDs
        assert operator['operator_name'] == 'Unknown'

    def test_complex_query_plan(self, collector):
        """Test parsing a complex query plan with multiple operators"""
        plan_tree = {
            "class": "org.apache.spark.sql.execution.aggregate.HashAggregateExec",
            "simpleString": "HashAggregate(keys=[], functions=[sum(revenue)])",
            "children": [
                {
                    "class": "org.apache.spark.sql.execution.exchange.ShuffleExchangeExec",
                    "simpleString": "Exchange SinglePartition",
                    "children": [
                        {
                            "class": "org.apache.spark.sql.execution.aggregate.HashAggregateExec",
                            "simpleString": "HashAggregate(keys=[], functions=[partial_sum(revenue)])",
                            "children": [
                                {
                                    "class": "org.apache.spark.sql.execution.ProjectExec",
                                    "simpleString": "Project [revenue]",
                                    "children": [
                                        {
                                            "class": "org.apache.spark.sql.execution.joins.BroadcastHashJoinExec",
                                            "simpleString": "BroadcastHashJoin [id], [id]",
                                            "children": [
                                                {
                                                    "class": "org.apache.spark.sql.execution.FileSourceScanExec",
                                                    "simpleString": "FileScan parquet orders",
                                                    "children": []
                                                },
                                                {
                                                    "class": "org.apache.spark.sql.execution.FileSourceScanExec",
                                                    "simpleString": "FileScan parquet customers",
                                                    "children": []
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        operators = collector._parse_plan_tree(plan_tree)

        # Should have: 2 HashAggregate, 1 Exchange, 1 Project, 1 Join, 2 Scans = 7 operators
        assert len(operators) == 7

        # Count operator types
        type_counts = {}
        for op in operators:
            op_type = op['operator_type']
            type_counts[op_type] = type_counts.get(op_type, 0) + 1

        assert type_counts['aggregate'] == 2
        assert type_counts['exchange'] == 1
        assert type_counts['project'] == 1
        assert type_counts['join'] == 1
        assert type_counts['scan'] == 2


class TestOperatorTypeClassification:
    """Test operator type classification with various Spark operator names"""

    @pytest.fixture
    def collector(self):
        """Create a collector for type classification tests"""
        spark = Mock()
        spark.sparkContext.applicationId = "test"
        spark.sparkContext.uiWebUrl = "http://localhost:4040"
        return OperatorMetricsCollector(spark)

    def test_scan_operators(self, collector):
        """Test classification of scan operators"""
        scan_operators = [
            "FileSourceScanExec",
            "BatchScanExec",
            "DataSourceScanExec",
            "InMemoryTableScanExec"
        ]

        for op_name in scan_operators:
            assert collector._get_operator_type_from_name(op_name) == "scan"

    def test_join_operators(self, collector):
        """Test classification of join operators"""
        join_operators = [
            "SortMergeJoinExec",
            "BroadcastHashJoinExec",
            "ShuffledHashJoinExec",
            "BroadcastNestedLoopJoinExec",
            "CartesianProductExec"
        ]

        for op_name in join_operators:
            assert collector._get_operator_type_from_name(op_name) == "join"

    def test_aggregate_operators(self, collector):
        """Test classification of aggregate operators"""
        agg_operators = [
            "HashAggregateExec",
            "ObjectHashAggregateExec",
            "SortAggregateExec"
        ]

        for op_name in agg_operators:
            assert collector._get_operator_type_from_name(op_name) == "aggregate"

    def test_exchange_operators(self, collector):
        """Test classification of exchange/shuffle operators"""
        exchange_operators = [
            "ShuffleExchangeExec",
            "BroadcastExchangeExec",
            "Exchange"
        ]

        for op_name in exchange_operators:
            assert collector._get_operator_type_from_name(op_name) == "exchange"

    def test_other_operators(self, collector):
        """Test classification of other common operators"""
        assert collector._get_operator_type_from_name("SortExec") == "sort"
        assert collector._get_operator_type_from_name("FilterExec") == "filter"
        assert collector._get_operator_type_from_name("ProjectExec") == "project"
        assert collector._get_operator_type_from_name("WholeStageCodegenExec") == "other"
        assert collector._get_operator_type_from_name("ColumnarToRowExec") == "other"


class TestEdgeCases:
    """Test edge cases and error handling"""

    @pytest.fixture
    def collector(self):
        """Create a collector for edge case tests"""
        spark = Mock()
        spark.sparkContext.applicationId = "test"
        spark.sparkContext.uiWebUrl = "http://localhost:4040"
        return OperatorMetricsCollector(spark)

    def test_empty_children_list(self, collector):
        """Test node with empty children list"""
        node = {
            "class": "org.apache.spark.sql.execution.FilterExec",
            "children": []
        }

        operator = collector._extract_operator_info_from_tree(node, node_id=0)
        assert operator['operator_id'] == 0  # Now using numeric IDs
        assert operator['children'] == []

    def test_missing_children_field(self, collector):
        """Test node without children field"""
        node = {
            "class": "org.apache.spark.sql.execution.FilterExec",
            "simpleString": "Filter (id > 10)"
        }

        operator = collector._extract_operator_info_from_tree(node, node_id=0)
        assert operator is not None
        assert operator['operator_id'] == 0  # Now using numeric IDs
        assert operator['children'] == []

    def test_none_node(self, collector):
        """Test handling of None node"""
        # Should not crash
        operators = collector._parse_plan_tree({})
        assert isinstance(operators, list)

    def test_recursive_depth(self, collector):
        """Test deeply nested plan tree"""
        # Create a chain of 10 operators
        node = {
            "class": "org.apache.spark.sql.execution.FileSourceScanExec",
            "children": []
        }

        for i in range(9):
            node = {
                "class": f"org.apache.spark.sql.execution.FilterExec",
                "children": [node]
            }

        operators = collector._parse_plan_tree(node)

        # Should have 10 operators
        assert len(operators) == 10

    def test_wide_tree(self, collector):
        """Test plan tree with many children"""
        # Create a node with 5 children
        children = [
            {
                "class": f"org.apache.spark.sql.execution.FileSourceScanExec",
                "simpleString": f"FileScan table{i}",
                "children": []
            }
            for i in range(5)
        ]

        node = {
            "class": "org.apache.spark.sql.execution.UnionExec",
            "children": children
        }

        operators = collector._parse_plan_tree(node)

        # Should have 6 operators: 1 Union + 5 Scans
        assert len(operators) == 6
