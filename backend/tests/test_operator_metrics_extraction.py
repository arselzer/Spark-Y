"""
Test to verify that operator metrics (especially row counts) are extracted correctly
"""

import json
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for testing."""
    spark = SparkSession.builder \
        .appName("OperatorMetricsExtractionTest") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()

    yield spark

    spark.stop()


def test_metrics_in_plan_tree(spark):
    """Test that metrics are extracted and included in the plan tree"""
    # Import the SparkManager to use its plan tree building method
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from app.spark.spark_manager import SparkManager

    # Create test data
    df = spark.createDataFrame([
        (1, "Alice", 100),
        (2, "Bob", 200),
        (3, "Charlie", 300),
        (4, "David", 400),
        (5, "Eve", 500)
    ], ["id", "name", "amount"])

    df.createOrReplaceTempView("test_table")

    # Execute a simple query with filter and projection
    query = "SELECT name, amount FROM test_table WHERE amount > 150"
    result_df = spark.sql(query)

    # Trigger execution by collecting results
    results = result_df.collect()
    print(f"\n{'='*80}")
    print(f"Query returned {len(results)} rows")
    print(f"{'='*80}")

    # Get the executed plan
    query_execution = result_df._jdf.queryExecution()
    executed_plan = query_execution.executedPlan()

    # Build plan tree using SparkManager's method
    manager = SparkManager()
    manager.spark = spark
    plan_tree = manager._build_plan_tree_from_operator(executed_plan)

    print(f"\n{'='*80}")
    print("PLAN TREE STRUCTURE")
    print(f"{'='*80}")
    print(json.dumps(plan_tree, indent=2, default=str))

    # Verify plan tree exists
    assert plan_tree is not None, "Plan tree should not be None"
    assert 'class' in plan_tree, "Plan tree should have 'class' field"
    assert 'metrics' in plan_tree, "Plan tree should have 'metrics' field"

    # Check if metrics are present
    print(f"\n{'='*80}")
    print("METRICS ANALYSIS")
    print(f"{'='*80}")

    def count_nodes_with_metrics(node, depth=0):
        """Recursively count nodes and check for metrics"""
        indent = "  " * depth
        node_class = node.get('class', 'Unknown').split('.')[-1]
        metrics = node.get('metrics', [])

        print(f"{indent}Node: {node_class}")
        print(f"{indent}  Metrics count: {len(metrics)}")

        metrics_found = {}
        for metric in metrics:
            if isinstance(metric, dict):
                name = metric.get('name', 'unknown')
                value = metric.get('value', 0)
                metrics_found[name] = value
                print(f"{indent}    {name}: {value}")

        # Recursively check children
        children = node.get('children', [])
        child_results = []
        for child in children:
            child_results.append(count_nodes_with_metrics(child, depth + 1))

        return {
            'node_class': node_class,
            'has_metrics': len(metrics) > 0,
            'metrics': metrics_found,
            'children': child_results
        }

    analysis = count_nodes_with_metrics(plan_tree)

    # Find nodes with output rows metric (numOutputRows)
    def find_output_row_metrics(node_analysis, results=None):
        if results is None:
            results = []

        metrics = node_analysis.get('metrics', {})
        # Check for various possible metric names
        for metric_name in ['numOutputRows', 'number of output rows', 'outputRows']:
            if metric_name in metrics:
                results.append({
                    'node': node_analysis['node_class'],
                    'rows': metrics[metric_name]
                })
                break  # Only add once per node

        for child in node_analysis.get('children', []):
            find_output_row_metrics(child, results)

        return results

    row_metrics = find_output_row_metrics(analysis)

    print(f"\n{'='*80}")
    print("NODES WITH OUTPUT ROW METRICS")
    print(f"{'='*80}")
    for item in row_metrics:
        print(f"  {item['node']}: {item['rows']} rows")

    # Assertions
    assert len(row_metrics) > 0, "At least one node should have 'number of output rows' metric"

    # Check that at least one node has non-zero rows
    non_zero_rows = [item for item in row_metrics if item['rows'] > 0]
    print(f"\n{'='*80}")
    print(f"Nodes with NON-ZERO row counts: {len(non_zero_rows)}")
    print(f"{'='*80}")

    assert len(non_zero_rows) > 0, "At least one node should have non-zero row count"

    print("\n✓ Metrics extraction test passed!")


def test_operator_metrics_collector(spark):
    """Test that OperatorMetricsCollector can extract row counts from plan tree"""
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from app.spark.spark_manager import SparkManager
    from app.spark.operator_metrics_collector import OperatorMetricsCollector

    # Create test data with known row count
    df = spark.createDataFrame([
        (i, f"Name_{i}", i * 100)
        for i in range(1, 11)  # 10 rows
    ], ["id", "name", "amount"])

    df.createOrReplaceTempView("test_data")

    # Execute query with filter (should return fewer rows)
    query = "SELECT id, name FROM test_data WHERE amount >= 500"
    result_df = spark.sql(query)

    # Trigger execution
    results = result_df.collect()
    expected_rows = len(results)
    print(f"\n{'='*80}")
    print(f"Query returned {expected_rows} rows")
    print(f"{'='*80}")

    # Build plan tree
    query_execution = result_df._jdf.queryExecution()
    executed_plan = query_execution.executedPlan()

    manager = SparkManager()
    manager.spark = spark
    plan_tree = manager._build_plan_tree_from_operator(executed_plan)

    # Use OperatorMetricsCollector to extract metrics
    collector = OperatorMetricsCollector(spark)
    operators = collector.collect_operator_metrics_from_plan(plan_tree)

    print(f"\n{'='*80}")
    print(f"OPERATOR METRICS COLLECTED")
    print(f"{'='*80}")
    print(f"Total operators: {len(operators)}")

    for i, op in enumerate(operators):
        print(f"\nOperator {i}:")
        print(f"  ID: {op['operator_id']}")
        print(f"  Name: {op['operator_name']}")
        print(f"  Type: {op['operator_type']}")
        print(f"  Output Rows: {op['num_output_rows']}")
        print(f"  Children: {op['children']}")

    # Assertions
    assert len(operators) > 0, "Should have at least one operator"

    # Check that at least one operator has non-zero rows
    operators_with_rows = [op for op in operators if op['num_output_rows'] > 0]

    print(f"\n{'='*80}")
    print(f"Operators with non-zero row counts: {len(operators_with_rows)}")
    print(f"{'='*80}")

    for op in operators_with_rows:
        print(f"  {op['operator_name']}: {op['num_output_rows']} rows")

    assert len(operators_with_rows) > 0, \
        "At least one operator should have non-zero row count after execution"

    # Find the operator that should have the final row count
    # This is typically the root operator
    root_operator = operators[0] if operators else None
    if root_operator:
        print(f"\n{'='*80}")
        print(f"Root operator row count: {root_operator['num_output_rows']}")
        print(f"Expected row count: {expected_rows}")
        print(f"{'='*80}")

    print("\n✓ OperatorMetricsCollector test passed!")


def test_join_query_metrics(spark):
    """Test metrics extraction for a join query (more complex plan)"""
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from app.spark.spark_manager import SparkManager
    from app.spark.operator_metrics_collector import OperatorMetricsCollector

    # Create two tables
    df1 = spark.createDataFrame([
        (1, "Alice"),
        (2, "Bob"),
        (3, "Charlie")
    ], ["id", "name"])

    df2 = spark.createDataFrame([
        (1, 100),
        (2, 200),
        (3, 300)
    ], ["id", "amount"])

    df1.createOrReplaceTempView("people")
    df2.createOrReplaceTempView("amounts")

    # Execute join query
    query = "SELECT p.name, a.amount FROM people p JOIN amounts a ON p.id = a.id"
    result_df = spark.sql(query)

    # Trigger execution
    results = result_df.collect()
    print(f"\n{'='*80}")
    print(f"Join query returned {len(results)} rows")
    print(f"{'='*80}")

    # Build plan tree and extract metrics
    query_execution = result_df._jdf.queryExecution()
    executed_plan = query_execution.executedPlan()

    manager = SparkManager()
    manager.spark = spark
    plan_tree = manager._build_plan_tree_from_operator(executed_plan)

    collector = OperatorMetricsCollector(spark)
    operators = collector.collect_operator_metrics_from_plan(plan_tree)

    print(f"\n{'='*80}")
    print(f"JOIN QUERY OPERATOR METRICS")
    print(f"{'='*80}")

    # Count operators by type
    operator_counts = {}
    for op in operators:
        op_type = op['operator_type']
        operator_counts[op_type] = operator_counts.get(op_type, 0) + 1

    print("Operator type counts:")
    for op_type, count in operator_counts.items():
        print(f"  {op_type}: {count}")

    # Find join operators
    join_operators = [op for op in operators if op['operator_type'] == 'join']

    print(f"\n{'='*80}")
    print(f"Found {len(join_operators)} join operator(s)")
    print(f"{'='*80}")

    for op in join_operators:
        print(f"  {op['operator_name']}: {op['num_output_rows']} rows")

    # Verify we have join operators
    assert len(join_operators) > 0, "Should have at least one join operator"

    print("\n✓ Join query metrics test passed!")
