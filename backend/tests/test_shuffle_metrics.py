"""
Test to discover actual metric names for Exchange/Shuffle operators in Spark
"""
import pytest
from pyspark.sql import SparkSession
from app.spark.operator_metrics_collector import OperatorMetricsCollector
import json


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for testing"""
    spark = SparkSession.builder \
        .appName("ShuffleMetricsTest") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.codegen.wholeStage", "false") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_discover_shuffle_metric_names(spark):
    """
    Test to discover what metrics are available on Exchange operators
    This will help us know the correct metric names to check for
    """
    # Create a simple dataset that will require a shuffle (groupBy)
    df = spark.createDataFrame([
        (1, "A", 100),
        (2, "B", 200),
        (3, "A", 300),
        (4, "B", 400),
        (5, "A", 500)
    ], ["id", "category", "value"])

    df.createOrReplaceTempView("test_data")

    # Query that will definitely create a shuffle (groupBy with aggregation)
    query = """
        SELECT category, SUM(value) as total
        FROM test_data
        GROUP BY category
    """

    result_df = spark.sql(query)

    # IMPORTANT: Execute the query FIRST to populate metrics
    result = result_df.collect()
    print(f"\nQuery result: {result}")

    # Now get the executed plan AFTER execution (when metrics are populated)
    query_execution = result_df._jdf.queryExecution()
    executed_plan = query_execution.executedPlan()

    # Build plan tree from the actual operator object (not JSON)
    # This is what spark_manager.py does to get metrics
    from app.spark.spark_manager import SparkManager
    manager = SparkManager()
    plan_tree = manager._build_plan_tree_from_operator(executed_plan)

    print(f"\nPlan tree built from operator object")

    # Function to recursively find Exchange operators and print their metrics
    def find_exchange_operators(node, depth=0):
        indent = "  " * depth

        if isinstance(node, dict):
            class_name = node.get('class', '')
            simple_name = class_name.split('.')[-1] if class_name else 'Unknown'

            print(f"{indent}Operator: {simple_name}")

            # Check if this is an Exchange operator
            if 'Exchange' in simple_name or 'Shuffle' in simple_name:
                print(f"{indent}*** FOUND EXCHANGE OPERATOR: {simple_name} ***")

                # Print all metrics
                metrics = node.get('metrics', [])
                print(f"{indent}Number of metrics: {len(metrics)}")

                if metrics:
                    print(f"{indent}All metrics:")
                    for metric in metrics:
                        if isinstance(metric, dict):
                            name = metric.get('name', 'unknown')
                            value = metric.get('value', 0)
                            print(f"{indent}  - {name}: {value}")
                else:
                    print(f"{indent}  (No metrics available)")

            # Recurse into children
            for child in node.get('children', []):
                find_exchange_operators(child, depth + 1)

    print("\n" + "="*80)
    print("SEARCHING FOR EXCHANGE OPERATORS AND THEIR METRICS")
    print("="*80)
    find_exchange_operators(plan_tree)

    # Also use the OperatorMetricsCollector to see what it extracts
    print("\n" + "="*80)
    print("USING OperatorMetricsCollector")
    print("="*80)

    collector = OperatorMetricsCollector(spark)
    operators = collector.collect_operator_metrics_from_plan(plan_tree)

    print(f"\nFound {len(operators)} operators")
    for op in operators:
        print(f"\nOperator: {op['operator_name']}")
        print(f"  Type: {op['operator_type']}")
        print(f"  Output rows: {op['num_output_rows']}")
        print(f"  All metrics: {op['metrics']}")

        if op['operator_type'] == 'exchange':
            print(f"  *** THIS IS AN EXCHANGE OPERATOR ***")
            print(f"  Metrics keys: {list(op['metrics'].keys())}")


def test_shuffle_with_join(spark):
    """
    Test with a join operation that creates shuffle exchanges
    """
    # Create two datasets
    df1 = spark.createDataFrame([
        (1, "Alice"),
        (2, "Bob"),
        (3, "Charlie")
    ], ["id", "name"])

    df2 = spark.createDataFrame([
        (1, "Engineering"),
        (2, "Sales"),
        (3, "Marketing")
    ], ["id", "dept"])

    df1.createOrReplaceTempView("employees")
    df2.createOrReplaceTempView("departments")

    # Join query that will create shuffles
    query = """
        SELECT e.name, d.dept
        FROM employees e
        JOIN departments d ON e.id = d.id
    """

    result_df = spark.sql(query)
    result = result_df.collect()
    print(f"\nJoin query result: {result}")

    # Get plan
    query_execution = result_df._jdf.queryExecution()
    executed_plan = query_execution.executedPlan()
    plan_json = executed_plan.toJSON()
    plan_tree = json.loads(plan_json)

    if isinstance(plan_tree, list):
        plan_tree = plan_tree[0] if len(plan_tree) > 0 else plan_tree

    collector = OperatorMetricsCollector(spark)
    operators = collector.collect_operator_metrics_from_plan(plan_tree)

    print("\n" + "="*80)
    print("JOIN QUERY - EXCHANGE OPERATORS")
    print("="*80)

    exchange_ops = [op for op in operators if op['operator_type'] == 'exchange']
    print(f"\nFound {len(exchange_ops)} exchange operators")

    for op in exchange_ops:
        print(f"\nExchange: {op['operator_name']}")
        print(f"  Output rows: {op['num_output_rows']}")
        print(f"  Metric names: {list(op['metrics'].keys())}")
        for name, value in op['metrics'].items():
            print(f"    {name}: {value}")
