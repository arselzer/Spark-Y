"""
Test the specific IMDB query that should be acyclic
"""
import pytest
from pyspark.sql import SparkSession
from app.hypergraph.extractor import HypergraphExtractor


@pytest.fixture(scope="module")
def spark_session():
    """Create a Spark session for testing"""
    spark = (SparkSession.builder
             .appName("Test IMDB Query")
             .master("local[2]")
             .config("spark.driver.memory", "2g")
             .config("spark.executor.memory", "2g")
             .getOrCreate())

    # Create test tables matching IMDB schema
    spark.sql("DROP TABLE IF EXISTS keyword")
    spark.sql("DROP TABLE IF EXISTS movie_keyword")
    spark.sql("DROP TABLE IF EXISTS title")
    spark.sql("DROP TABLE IF EXISTS cast_info")
    spark.sql("DROP TABLE IF EXISTS name")

    # keyword table
    spark.sql("""
        CREATE TABLE keyword (
            id INT,
            keyword STRING
        ) USING parquet
    """)
    spark.sql("""
        INSERT INTO keyword VALUES
        (1, 'superhero'),
        (2, 'sequel')
    """)

    # movie_keyword table
    spark.sql("""
        CREATE TABLE movie_keyword (
            keyword_id INT,
            movie_id INT
        ) USING parquet
    """)
    spark.sql("""
        INSERT INTO movie_keyword VALUES
        (1, 100),
        (2, 101)
    """)

    # title table
    spark.sql("""
        CREATE TABLE title (
            id INT,
            title STRING,
            production_year INT
        ) USING parquet
    """)
    spark.sql("""
        INSERT INTO title VALUES
        (100, 'Iron Man', 2008),
        (101, 'Iron Man 2', 2010)
    """)

    # cast_info table
    spark.sql("""
        CREATE TABLE cast_info (
            movie_id INT,
            person_id INT
        ) USING parquet
    """)
    spark.sql("""
        INSERT INTO cast_info VALUES
        (100, 1),
        (101, 1)
    """)

    # name table
    spark.sql("""
        CREATE TABLE name (
            id INT,
            name STRING
        ) USING parquet
    """)
    spark.sql("""
        INSERT INTO name VALUES
        (1, 'Robert Downey Jr.')
    """)

    yield spark

    spark.stop()


def test_imdb_superhero_query(spark_session):
    """Test the IMDB superhero query - should be ACYCLIC"""

    query = """
    SELECT MIN(k.keyword) AS movie_keyword,
           MIN(n.name) AS actor_name,
           MIN(t.title) AS hero_movie
    FROM cast_info AS ci,
         keyword AS k,
         movie_keyword AS mk,
         name AS n,
         title AS t
    WHERE k.keyword IN ('superhero', 'sequel', 'second-part', 'marvel-comics',
                        'based-on-comic', 'tv-special', 'fight', 'violence')
      AND n.name LIKE '%Downey%Robert%'
      AND t.production_year > 2000
      AND k.id = mk.keyword_id
      AND t.id = mk.movie_id
      AND t.id = ci.movie_id
      AND ci.movie_id = mk.movie_id
      AND n.id = ci.person_id
    """

    extractor = HypergraphExtractor(spark_session)
    hypergraph = extractor.extract_hypergraph(query, "test_imdb_query")

    # Debug output
    print("\n" + "=" * 80)
    print("HYPERGRAPH STRUCTURE")
    print("=" * 80)

    print(f"\nNodes ({len(hypergraph.nodes)}):")
    for node in hypergraph.nodes:
        print(f"  - {node.id}: {node.attributes}")

    print(f"\nEdges ({len(hypergraph.edges)}):")
    for edge in hypergraph.edges:
        print(f"  - {edge.id}: connects nodes {edge.nodes}")

    print(f"\nIs Acyclic: {hypergraph.is_acyclic}")
    print(f"Join Tree: {hypergraph.join_tree}")

    if hypergraph.gyo_steps:
        print(f"\nGYO Steps ({len(hypergraph.gyo_steps)}):")
        for step in hypergraph.gyo_steps:
            print(f"  Step {step.step_number}: {step.action}")
            print(f"    Description: {step.description}")
            print(f"    Removed edges: {step.removed_edges}")
            print(f"    Remaining edges: {step.remaining_edges}")

    print("=" * 80)

    # Assertions
    assert hypergraph.is_acyclic, (
        f"Query should be ACYCLIC but was classified as CYCLIC. "
        f"Nodes: {len(hypergraph.nodes)}, Edges: {len(hypergraph.edges)}"
    )
    assert hypergraph.join_tree is not None, "Join tree should be generated for acyclic query"
    assert len(hypergraph.join_tree) > 0, "Join tree should not be empty"


def test_simple_chain_query(spark_session):
    """Test a simple chain query - definitely acyclic"""

    query = """
    SELECT COUNT(*)
    FROM title t
    JOIN movie_keyword mk ON t.id = mk.movie_id
    JOIN keyword k ON mk.keyword_id = k.id
    """

    extractor = HypergraphExtractor(spark_session)
    hypergraph = extractor.extract_hypergraph(query, "test_chain_query")

    print("\n" + "=" * 80)
    print("SIMPLE CHAIN QUERY")
    print("=" * 80)
    print(f"Nodes: {len(hypergraph.nodes)}")
    print(f"Edges: {len(hypergraph.edges)}")
    print(f"Is Acyclic: {hypergraph.is_acyclic}")
    print("=" * 80)

    assert hypergraph.is_acyclic, "Simple chain query should be acyclic"
    assert hypergraph.join_tree is not None, "Join tree should exist"


def test_disconnected_forest_fix(spark_session):
    """
    Test the fix for disconnected join tree forest bug.

    This query used to create a forest with multiple roots:
    - mc (root with children cn and t)
    - mk (root with child k)

    After the fix, it should create a single connected tree.
    """
    # First create the additional tables needed for this test
    spark_session.sql("DROP TABLE IF EXISTS company_name")
    spark_session.sql("DROP TABLE IF EXISTS movie_companies")

    spark_session.sql("""
        CREATE TABLE company_name (
            id INT,
            name STRING,
            country_code STRING
        ) USING parquet
    """)
    spark_session.sql("""
        INSERT INTO company_name VALUES
        (1, 'Marvel Studios', '[us]'),
        (2, 'Warner Bros', '[us]')
    """)

    spark_session.sql("""
        CREATE TABLE movie_companies (
            company_id INT,
            movie_id INT
        ) USING parquet
    """)
    spark_session.sql("""
        INSERT INTO movie_companies VALUES
        (1, 100),
        (2, 101)
    """)

    query = """
    SELECT MIN(t.title) AS movie_title
    FROM company_name AS cn,
         keyword AS k,
         movie_companies AS mc,
         movie_keyword AS mk,
         title AS t
    WHERE cn.country_code ='[us]'
      AND k.keyword ='superhero'
      AND cn.id = mc.company_id
      AND mc.movie_id = t.id
      AND t.id = mk.movie_id
      AND mk.keyword_id = k.id
      AND mc.movie_id = mk.movie_id
    """

    extractor = HypergraphExtractor(spark_session)
    hypergraph = extractor.extract_hypergraph(query, "test_disconnected_forest")

    print("\n" + "=" * 80)
    print("DISCONNECTED FOREST FIX TEST")
    print("=" * 80)
    print(f"Nodes: {len(hypergraph.nodes)}")
    print(f"Edges: {len(hypergraph.edges)}")
    print(f"Is Acyclic: {hypergraph.is_acyclic}")

    if hypergraph.join_tree:
        print(f"\nJoin Tree ({len(hypergraph.join_tree)} nodes):")
        # Count roots
        roots = [node for node in hypergraph.join_tree if node.parent is None]
        print(f"Number of roots: {len(roots)}")
        for root in roots:
            print(f"  Root: {root.relation} (id: {root.id})")
            print(f"    Children: {root.children}")

        # Print full tree structure
        print("\nFull tree structure:")
        for node in hypergraph.join_tree:
            print(f"  {node.relation}:")
            print(f"    Parent: {node.parent}")
            print(f"    Children: {node.children}")
            print(f"    Level: {node.level}")
            if node.shared_attributes:
                print(f"    Shared attributes: {node.shared_attributes}")

    print("=" * 80)

    # Assertions
    assert hypergraph.is_acyclic, "This query should be acyclic (connected chain/tree)"
    assert hypergraph.join_tree is not None, "Join tree should exist"

    # CRITICAL: The join tree should have exactly ONE root (connected tree, not forest)
    roots = [node for node in hypergraph.join_tree if node.parent is None]
    assert len(roots) == 1, (
        f"Join tree should have exactly 1 root (connected tree), "
        f"but found {len(roots)} roots: {[r.relation for r in roots]}. "
        f"This indicates a disconnected forest."
    )
