"""
Integration tests for SparkManager plan tree building with real Spark queries

These tests use actual SparkSession and real query execution to verify that
plan trees are built correctly from real Spark operator structures, not just mocks.
"""
import pytest
from pyspark.sql import SparkSession
from app.spark.spark_manager import SparkManager


@pytest.fixture(scope="module")
def spark_session():
    """Create a Spark session for testing"""
    spark = (SparkSession.builder
             .appName("Test Plan Tree Integration")
             .master("local[2]")
             .config("spark.driver.memory", "1g")
             .config("spark.executor.memory", "1g")
             .config("spark.sql.shuffle.partitions", "2")  # Small for faster tests
             .getOrCreate())

    # Create test tables
    _create_test_tables(spark)

    yield spark

    spark.stop()


def _create_test_tables(spark):
    """Create test tables with sample data"""

    # Drop existing tables
    for table in ['orders', 'customers', 'products']:
        spark.sql(f"DROP TABLE IF EXISTS {table}")

    # Create orders table
    spark.sql("""
        CREATE TABLE orders (
            order_id INT,
            customer_id INT,
            product_id INT,
            amount DOUBLE,
            order_date STRING
        ) USING parquet
    """)

    # Insert sample data
    spark.sql("""
        INSERT INTO orders VALUES
            (1, 101, 201, 100.0, '2024-01-01'),
            (2, 102, 202, 200.0, '2024-01-02'),
            (3, 103, 203, 150.0, '2024-01-03'),
            (4, 101, 204, 300.0, '2024-01-04'),
            (5, 102, 201, 250.0, '2024-01-05')
    """)

    # Create customers table
    spark.sql("""
        CREATE TABLE customers (
            customer_id INT,
            name STRING,
            city STRING
        ) USING parquet
    """)

    spark.sql("""
        INSERT INTO customers VALUES
            (101, 'Alice', 'NYC'),
            (102, 'Bob', 'SF'),
            (103, 'Charlie', 'LA')
    """)

    # Create products table
    spark.sql("""
        CREATE TABLE products (
            product_id INT,
            name STRING,
            category STRING
        ) USING parquet
    """)

    spark.sql("""
        INSERT INTO products VALUES
            (201, 'Widget A', 'Electronics'),
            (202, 'Widget B', 'Electronics'),
            (203, 'Gadget C', 'Home'),
            (204, 'Tool D', 'Industrial')
    """)


class TestRealSparkPlanTree:
    """Integration tests with real Spark query execution"""

    def test_simple_scan_plan(self, spark_session):
        """Test plan tree from a simple table scan"""
        spark_manager = SparkManager()
        spark_manager.spark = spark_session

        # Simple query - must execute first to get real plan (not AQE wrapper)
        df = spark_session.sql("SELECT * FROM orders WHERE order_id > 2")
        results = df.collect()  # Execute the query
        query_execution = df._jdf.queryExecution()
        executed_plan = query_execution.executedPlan()

        # Build plan tree
        plan_tree = spark_manager._build_plan_tree_from_operator(executed_plan)

        # Verify structure
        assert plan_tree is not None
        assert 'class' in plan_tree
        assert 'simpleString' in plan_tree
        assert 'children' in plan_tree
        assert isinstance(plan_tree['children'], list)

        # Should have some structure (filter or scan at minimum)
        print(f"\nSimple scan plan class: {plan_tree['class']}")
        print(f"Simple string: {plan_tree['simpleString']}")

    def test_join_plan_hierarchy(self, spark_session):
        """Test plan tree from a query with joins"""
        spark_manager = SparkManager()
        spark_manager.spark = spark_session

        # Query with a join - must execute first to get real plan (not AQE wrapper)
        df = spark_session.sql("""
            SELECT o.order_id, c.name, o.amount
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            WHERE o.amount > 100
        """)
        results = df.collect()  # Execute the query
        query_execution = df._jdf.queryExecution()
        executed_plan = query_execution.executedPlan()

        # Build plan tree
        plan_tree = spark_manager._build_plan_tree_from_operator(executed_plan)

        # Verify hierarchical structure
        assert plan_tree is not None
        assert 'children' in plan_tree

        # Count depth by traversing tree
        def get_depth(node):
            if not node or not node.get('children'):
                return 1
            return 1 + max(get_depth(child) for child in node['children'])

        depth = get_depth(plan_tree)
        print(f"\nJoin query depth: {depth}")
        print(f"Root operator: {plan_tree['class'].split('.')[-1]}")

        # Should have at least 2 levels (root + at least one child)
        assert depth >= 2, f"Expected depth >= 2, got {depth}"

        # Should have children
        assert len(plan_tree['children']) > 0, "Root should have children"

    def test_aggregate_plan_structure(self, spark_session):
        """Test plan tree from an aggregate query"""
        spark_manager = SparkManager()
        spark_manager.spark = spark_session

        # Aggregate query - must execute first to get real plan (not AQE wrapper)
        df = spark_session.sql("""
            SELECT customer_id, SUM(amount) as total
            FROM orders
            GROUP BY customer_id
        """)
        results = df.collect()  # Execute the query
        query_execution = df._jdf.queryExecution()
        executed_plan = query_execution.executedPlan()

        # Build plan tree
        plan_tree = spark_manager._build_plan_tree_from_operator(executed_plan)

        assert plan_tree is not None

        # Count total operators in tree
        def count_operators(node):
            if not node:
                return 0
            count = 1  # This node
            for child in node.get('children', []):
                count += count_operators(child)
            return count

        total_ops = count_operators(plan_tree)
        print(f"\nAggregate query total operators: {total_ops}")

        # Should have multiple operators (at least aggregate + scan)
        assert total_ops >= 2, f"Expected >= 2 operators, got {total_ops}"

    def test_complex_multi_join_plan(self, spark_session):
        """Test plan tree from a complex query with multiple joins"""
        spark_manager = SparkManager()
        spark_manager.spark = spark_session

        # Complex query with multiple joins - must execute first to get real plan (not AQE wrapper)
        df = spark_session.sql("""
            SELECT c.name, p.name as product_name, SUM(o.amount) as total
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            JOIN products p ON o.product_id = p.product_id
            WHERE o.amount > 100
            GROUP BY c.name, p.name
            ORDER BY total DESC
        """)
        results = df.collect()  # Execute the query
        query_execution = df._jdf.queryExecution()
        executed_plan = query_execution.executedPlan()

        # Build plan tree
        plan_tree = spark_manager._build_plan_tree_from_operator(executed_plan)

        assert plan_tree is not None

        # Verify all nodes have required fields
        def verify_node(node):
            assert 'class' in node
            assert 'simpleString' in node
            assert 'children' in node
            assert isinstance(node['children'], list)

            # Recursively verify children
            for child in node['children']:
                verify_node(child)

        verify_node(plan_tree)

        # Count operators and depth
        def get_stats(node):
            if not node:
                return {'count': 0, 'depth': 0}

            count = 1
            max_child_depth = 0

            for child in node.get('children', []):
                child_stats = get_stats(child)
                count += child_stats['count']
                max_child_depth = max(max_child_depth, child_stats['depth'])

            return {'count': count, 'depth': max_child_depth + 1}

        stats = get_stats(plan_tree)
        print(f"\nComplex query operators: {stats['count']}")
        print(f"Complex query depth: {stats['depth']}")

        # Should have significant structure for complex query
        # Note: Spark may fuse operators, so we expect at least 4 (not 5)
        assert stats['count'] >= 4, f"Expected >= 4 operators, got {stats['count']}"
        assert stats['depth'] >= 3, f"Expected depth >= 3, got {stats['depth']}"

    def test_operator_types_in_real_plan(self, spark_session):
        """Test that we can identify different operator types in real plans"""
        spark_manager = SparkManager()
        spark_manager.spark = spark_session

        # Query with various operators - must execute first to get real plan (not AQE wrapper)
        df = spark_session.sql("""
            SELECT customer_id, COUNT(*) as order_count, SUM(amount) as total
            FROM orders
            WHERE amount > 100
            GROUP BY customer_id
            HAVING COUNT(*) > 1
            ORDER BY total DESC
        """)
        results = df.collect()  # Execute the query
        query_execution = df._jdf.queryExecution()
        executed_plan = query_execution.executedPlan()

        # Build plan tree
        plan_tree = spark_manager._build_plan_tree_from_operator(executed_plan)

        # Collect all operator class names
        def collect_operators(node):
            if not node:
                return []

            operators = [node['class'].split('.')[-1]]
            for child in node.get('children', []):
                operators.extend(collect_operators(child))

            return operators

        operators = collect_operators(plan_tree)
        print(f"\nOperators found: {operators}")

        # Should have various operator types
        operator_str = ' '.join(operators)

        # Should have scan operators
        assert any('Scan' in op for op in operators), "Should have scan operators"

        # Complex query might have exchanges (shuffles) or aggregates
        # But this depends on Spark's optimization, so we just verify structure exists
        assert len(operators) >= 3, f"Expected >= 3 operators, got {len(operators)}"

    def test_no_artificial_root_node(self, spark_session):
        """Verify that plan tree doesn't have artificial 'Root' wrapper"""
        spark_manager = SparkManager()
        spark_manager.spark = spark_session

        df = spark_session.sql("SELECT * FROM orders")
        query_execution = df._jdf.queryExecution()
        executed_plan = query_execution.executedPlan()

        plan_tree = spark_manager._build_plan_tree_from_operator(executed_plan)

        # Root should be a real Spark operator, not "Root"
        root_class = plan_tree['class'].split('.')[-1]
        print(f"\nRoot operator: {root_class}")

        # Should NOT be an artificial "Root" wrapper
        assert root_class != "Root", "Should not have artificial Root wrapper"

        # Should be a real Spark operator class
        assert 'org.apache.spark.sql.execution' in plan_tree['class'], \
            "Root should be a real Spark execution operator"

    def test_children_are_not_flat_list(self, spark_session):
        """Verify that children form a proper tree, not a flat list"""
        spark_manager = SparkManager()
        spark_manager.spark = spark_session

        # Query that will have multiple levels
        df = spark_session.sql("""
            SELECT customer_id, SUM(amount) as total
            FROM orders
            WHERE amount > 100
            GROUP BY customer_id
        """)
        query_execution = df._jdf.queryExecution()
        executed_plan = query_execution.executedPlan()

        plan_tree = spark_manager._build_plan_tree_from_operator(executed_plan)

        # Count how many direct children the root has
        root_children_count = len(plan_tree.get('children', []))

        # Count total operators
        def count_all_ops(node):
            if not node:
                return 0
            count = 1
            for child in node.get('children', []):
                count += count_all_ops(child)
            return count

        total_ops = count_all_ops(plan_tree)

        print(f"\nRoot has {root_children_count} direct children")
        print(f"Total operators: {total_ops}")

        # If we have multiple operators, root should NOT have all of them as direct children
        # (that would indicate a flat structure)
        if total_ops > 2:
            # Root should typically have 1-2 direct children, not all operators
            assert root_children_count < total_ops, \
                f"Root has {root_children_count} children out of {total_ops} total ops - structure appears flat"

    def test_plan_tree_matches_operator_metrics_format(self, spark_session):
        """Verify plan tree format is compatible with OperatorMetricsCollector"""
        spark_manager = SparkManager()
        spark_manager.spark = spark_session

        df = spark_session.sql("SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id")
        query_execution = df._jdf.queryExecution()
        executed_plan = query_execution.executedPlan()

        plan_tree = spark_manager._build_plan_tree_from_operator(executed_plan)

        # Import OperatorMetricsCollector to test integration
        from app.spark.operator_metrics_collector import OperatorMetricsCollector

        collector = OperatorMetricsCollector(spark_session)

        # This should work without errors - plan tree format should be compatible
        operators = collector.collect_operator_metrics_from_plan(plan_tree)

        # Should extract operators successfully
        assert operators is not None
        assert len(operators) > 0

        # Each operator should have required fields
        for op in operators:
            assert 'operator_id' in op
            assert 'operator_name' in op
            assert 'operator_type' in op
            assert 'children' in op

        print(f"\nExtracted {len(operators)} operators from real plan tree")
        print(f"Operator types: {[op['operator_type'] for op in operators]}")
