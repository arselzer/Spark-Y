"""
Test to understand the format of Spark's logical plan JSON output.

This test helps diagnose issues with displaying logical plans in the frontend
by examining the actual structure returned by Spark's toJSON() method.
"""

import json
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for testing."""
    spark = SparkSession.builder \
        .appName("LogicalPlanJSONFormatTest") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()

    yield spark

    spark.stop()


def test_logical_plan_json_structure(spark):
    """
    Test the structure of Spark's logical plan JSON to understand
    how to properly parse it in the frontend.
    """
    # Create simple test data
    df1 = spark.createDataFrame([
        (1, "Alice", 100),
        (2, "Bob", 200),
        (3, "Charlie", 300)
    ], ["id", "name", "amount"])

    df2 = spark.createDataFrame([
        (1, "Engineering"),
        (2, "Sales"),
        (3, "Marketing")
    ], ["id", "department"])

    df1.createOrReplaceTempView("employees")
    df2.createOrReplaceTempView("departments")

    # Execute a query with joins and aggregates (similar to what the app uses)
    query = """
        SELECT d.department, MIN(e.name) as min_name
        FROM employees e
        JOIN departments d ON e.id = d.id
        GROUP BY d.department
    """

    df = spark.sql(query)
    query_execution = df._jdf.queryExecution()

    # Get analyzed logical plan JSON
    analyzed_json_str = query_execution.analyzed().toJSON()
    print(f"\n{'='*80}")
    print("ANALYZED LOGICAL PLAN JSON")
    print(f"{'='*80}")
    print(f"JSON string length: {len(analyzed_json_str)}")

    analyzed_parsed = json.loads(analyzed_json_str)

    print(f"Type: {type(analyzed_parsed)}")
    print(f"Is list: {isinstance(analyzed_parsed, list)}")
    print(f"Is dict: {isinstance(analyzed_parsed, dict)}")

    # Test 1: Check basic structure
    if isinstance(analyzed_parsed, list):
        print(f"\n✓ Parsed as LIST with {len(analyzed_parsed)} elements")
        assert len(analyzed_parsed) > 0, "Logical plan array should not be empty"

        # Test 2: Check first element structure
        first_elem = analyzed_parsed[0]
        print(f"\nFirst element type: {type(first_elem)}")
        assert isinstance(first_elem, dict), "First element should be a dictionary"

        print(f"First element keys: {list(first_elem.keys())}")
        print(f"First element class: {first_elem.get('class', 'N/A')}")
        print(f"First element num-children: {first_elem.get('num-children', 'N/A')}")

        # Test 3: Check child reference pattern
        child_refs = []
        left_refs = []
        right_refs = []

        for i, elem in enumerate(analyzed_parsed[:10]):  # Check first 10 elements
            if 'child' in elem:
                child_refs.append((i, elem['child']))
            if 'left' in elem:
                left_refs.append((i, elem['left']))
            if 'right' in elem:
                right_refs.append((i, elem['right']))

        print(f"\nChild references in first 10 elements: {child_refs}")
        print(f"Left references in first 10 elements: {left_refs}")
        print(f"Right references in first 10 elements: {right_refs}")

        # Test 4: Check if children arrays are populated
        elements_with_children = []
        for i, elem in enumerate(analyzed_parsed):
            if 'children' in elem and isinstance(elem['children'], list) and len(elem['children']) > 0:
                elements_with_children.append((i, len(elem['children']), type(elem['children'][0])))

        print(f"\nElements with non-empty 'children' arrays: {len(elements_with_children)}")
        if elements_with_children:
            print("Examples (index, count, first_child_type):")
            for item in elements_with_children[:5]:
                print(f"  {item}")

        # Test 5: Detailed structure of first few elements
        print(f"\n{'='*80}")
        print("FIRST 3 ELEMENTS (FULL STRUCTURE)")
        print(f"{'='*80}")
        for i in range(min(3, len(analyzed_parsed))):
            print(f"\n--- Element {i} ---")
            print(json.dumps(analyzed_parsed[i], indent=2))

    elif isinstance(analyzed_parsed, dict):
        print(f"\n✓ Parsed as DICT (single root node)")
        print(f"Root keys: {list(analyzed_parsed.keys())}")
        print(f"Root class: {analyzed_parsed.get('class', 'N/A')}")

        if 'children' in analyzed_parsed:
            print(f"Root has 'children' field: Yes")
            print(f"Children type: {type(analyzed_parsed['children'])}")
            if isinstance(analyzed_parsed['children'], list):
                print(f"Children length: {len(analyzed_parsed['children'])}")

    # Get optimized logical plan for comparison
    optimized_json_str = query_execution.optimizedPlan().toJSON()
    optimized_parsed = json.loads(optimized_json_str)

    print(f"\n{'='*80}")
    print("OPTIMIZED LOGICAL PLAN JSON")
    print(f"{'='*80}")
    print(f"Same structure as analyzed: {type(optimized_parsed) == type(analyzed_parsed)}")

    if isinstance(optimized_parsed, list):
        print(f"Optimized list length: {len(optimized_parsed)}")
        print(f"Analyzed list length: {len(analyzed_parsed)}")

    # Get physical plan for comparison
    physical_json_str = query_execution.executedPlan().toJSON()
    physical_parsed = json.loads(physical_json_str)

    print(f"\n{'='*80}")
    print("PHYSICAL PLAN JSON (for comparison)")
    print(f"{'='*80}")
    print(f"Physical plan type: {type(physical_parsed)}")
    print(f"Physical plan is dict: {isinstance(physical_parsed, dict)}")

    if isinstance(physical_parsed, dict):
        print(f"Physical plan keys: {list(physical_parsed.keys())}")
        if 'children' in physical_parsed:
            print(f"Physical plan children type: {type(physical_parsed['children'])}")

    # Summary and recommendations
    print(f"\n{'='*80}")
    print("SUMMARY AND RECOMMENDATIONS")
    print(f"{'='*80}")

    if isinstance(analyzed_parsed, list):
        # Check if all child/left/right refs point to indices 0-1
        all_refs = set()
        for elem in analyzed_parsed[:20]:
            if 'child' in elem and isinstance(elem['child'], (int, float)):
                all_refs.add(int(elem['child']))
            if 'left' in elem and isinstance(elem['left'], (int, float)):
                all_refs.add(int(elem['left']))
            if 'right' in elem and isinstance(elem['right'], (int, float)):
                all_refs.add(int(elem['right']))

        print(f"Unique numeric references in first 20 elements: {sorted(all_refs)}")

        if len(all_refs) <= 3 and all_refs <= {0, 1, 2}:
            print("\n⚠ WARNING: Most/all numeric references are 0-2!")
            print("This suggests these are NOT array indices but rather:")
            print("  1. Child position indicators (0=first child, 1=second child)")
            print("  2. Sentinel/placeholder values")
            print("  3. The logical plan uses a different format than physical plans")
            print("\nRECOMMENDATION: Don't treat child/left/right as array indices for logical plans!")

    # Assert basic structure is valid
    assert analyzed_parsed is not None, "Logical plan should not be None"
    assert optimized_parsed is not None, "Optimized plan should not be None"
    assert isinstance(analyzed_parsed, (list, dict)), "Plan should be list or dict"

    print("\n✓ Test completed successfully!")
