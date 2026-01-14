"""
Test case for the movie_link query that's incorrectly classified as cyclic

This tests a query pattern with two star-like structures connected through
a central table (movie_link), which should be acyclic.
"""
import pytest
from pyspark.sql import SparkSession
from app.hypergraph.extractor import HypergraphExtractor


@pytest.fixture(scope="module")
def spark_session():
    """Create a Spark session for testing"""
    spark = (SparkSession.builder
             .appName("Test Movie Link Query")
             .master("local[2]")
             .config("spark.driver.memory", "2g")
             .config("spark.executor.memory", "2g")
             .getOrCreate())

    # Create all required tables matching IMDB schema

    # company_name table
    spark.sql("DROP TABLE IF EXISTS company_name")
    spark.sql("""
        CREATE TABLE company_name (
            id INT,
            name STRING,
            country_code STRING
        ) USING parquet
    """)
    spark.sql("""
        INSERT INTO company_name VALUES
        (1, 'Marvel Studios', '[us]'),
        (2, 'Warner Bros', '[us]')
    """)

    # company_type table
    spark.sql("DROP TABLE IF EXISTS company_type")
    spark.sql("""
        CREATE TABLE company_type (
            id INT,
            kind STRING
        ) USING parquet
    """)
    spark.sql("""
        INSERT INTO company_type VALUES
        (1, 'production companies'),
        (2, 'distributors')
    """)

    # info_type table
    spark.sql("DROP TABLE IF EXISTS info_type")
    spark.sql("""
        CREATE TABLE info_type (
            id INT,
            info STRING
        ) USING parquet
    """)
    spark.sql("""
        INSERT INTO info_type VALUES
        (1, 'budget'),
        (2, 'bottom 10 rank'),
        (3, 'rating')
    """)

    # kind_type table
    spark.sql("DROP TABLE IF EXISTS kind_type")
    spark.sql("""
        CREATE TABLE kind_type (
            id INT,
            kind STRING
        ) USING parquet
    """)
    spark.sql("""
        INSERT INTO kind_type VALUES
        (1, 'tv series'),
        (2, 'tv series')
    """)

    # link_type table
    spark.sql("DROP TABLE IF EXISTS link_type")
    spark.sql("""
        CREATE TABLE link_type (
            id INT,
            link STRING
        ) USING parquet
    """)
    spark.sql("""
        INSERT INTO link_type VALUES
        (1, 'sequel'),
        (2, 'follows')
    """)

    # movie_companies table
    spark.sql("DROP TABLE IF EXISTS movie_companies")
    spark.sql("""
        CREATE TABLE movie_companies (
            company_id INT,
            movie_id INT,
            company_type_id INT
        ) USING parquet
    """)
    spark.sql("""
        INSERT INTO movie_companies VALUES
        (1, 100, 1),
        (2, 101, 2)
    """)

    # movie_info_idx table
    spark.sql("DROP TABLE IF EXISTS movie_info_idx")
    spark.sql("""
        CREATE TABLE movie_info_idx (
            info_type_id INT,
            movie_id INT,
            info STRING
        ) USING parquet
    """)
    spark.sql("""
        INSERT INTO movie_info_idx VALUES
        (1, 100, '8.5'),
        (2, 101, '2.5')
    """)

    # movie_link table (central connector)
    spark.sql("DROP TABLE IF EXISTS movie_link")
    spark.sql("""
        CREATE TABLE movie_link (
            link_type_id INT,
            movie_id INT,
            linked_movie_id INT
        ) USING parquet
    """)
    spark.sql("""
        INSERT INTO movie_link VALUES
        (1, 100, 101)
    """)

    # title table
    spark.sql("DROP TABLE IF EXISTS title")
    spark.sql("""
        CREATE TABLE title (
            id INT,
            title STRING,
            production_year INT,
            kind_id INT
        ) USING parquet
    """)
    spark.sql("""
        INSERT INTO title VALUES
        (100, 'Birdemic: Shock Movie', 2008, 1),
        (101, 'Breaking Bad 2', 2006, 2)
    """)

    # movie_info table
    spark.sql("DROP TABLE IF EXISTS movie_info")
    spark.sql("""
        CREATE TABLE movie_info (
            movie_id INT,
            info_type_id INT,
            info STRING
        ) USING parquet
    """)
    spark.sql("""
        INSERT INTO movie_info VALUES
        (100, 1, '1000000'),
        (101, 1, '500000')
    """)

    yield spark

    spark.stop()


def test_movie_link_query_should_be_acyclic(spark_session):
    """
    Test the movie_link query - should be ACYCLIC

    This query has two star-like structures (one for each movie) connected
    through the movie_link table. It should be acyclic.

    Structure:
        Left side (movie 1):         Center:         Right side (movie 2):
            cn1 ----                   lt                  cn2 ----
                    |                   |                           |
            it1 --- mc1 --- t1 ------- ml ------- t2 --- mc2 --- it2
                    |         |                      |         |
            kt1 ----    mi_idx1                 mi_idx2    ---- kt2
    """

    query = """
    SELECT MIN(cn1.name) AS first_company,
           MIN(cn2.name) AS second_company,
           MIN(mi_idx1.info) AS first_rating,
           MIN(mi_idx2.info) AS second_rating,
           MIN(t1.title) AS first_movie,
           MIN(t2.title) AS second_movie
    FROM company_name AS cn1,
         company_name AS cn2,
         info_type AS it1,
         info_type AS it2,
         kind_type AS kt1,
         kind_type AS kt2,
         link_type AS lt,
         movie_companies AS mc1,
         movie_companies AS mc2,
         movie_info_idx AS mi_idx1,
         movie_info_idx AS mi_idx2,
         movie_link AS ml,
         title AS t1,
         title AS t2
    WHERE cn1.country_code = '[us]'
      AND it1.info = 'rating'
      AND it2.info = 'rating'
      AND kt1.kind IN ('tv series')
      AND kt2.kind IN ('tv series')
      AND lt.link IN ('sequel', 'follows', 'followed by')
      AND mi_idx2.info < '3.0'
      AND t2.production_year BETWEEN 2005 AND 2008
      AND lt.id = ml.link_type_id
      AND t1.id = ml.movie_id
      AND t2.id = ml.linked_movie_id
      AND it1.id = mi_idx1.info_type_id
      AND t1.id = mi_idx1.movie_id
      AND kt1.id = t1.kind_id
      AND cn1.id = mc1.company_id
      AND t1.id = mc1.movie_id
      AND ml.movie_id = mi_idx1.movie_id
      AND ml.movie_id = mc1.movie_id
      AND mi_idx1.movie_id = mc1.movie_id
      AND it2.id = mi_idx2.info_type_id
      AND t2.id = mi_idx2.movie_id
      AND kt2.id = t2.kind_id
      AND cn2.id = mc2.company_id
      AND t2.id = mc2.movie_id
      AND ml.linked_movie_id = mi_idx2.movie_id
      AND ml.linked_movie_id = mc2.movie_id
      AND mi_idx2.movie_id = mc2.movie_id
    """

    extractor = HypergraphExtractor(spark_session)
    hypergraph = extractor.extract_hypergraph(query, "test_movie_link_query")

    # Debug output
    print("\n" + "=" * 80)
    print("MOVIE LINK QUERY HYPERGRAPH STRUCTURE")
    print("=" * 80)

    print(f"\nNodes ({len(hypergraph.nodes)}):")
    for node in hypergraph.nodes:
        print(f"  - {node.id}: {node.attributes}")

    print(f"\nEdges ({len(hypergraph.edges)}):")
    for edge in hypergraph.edges:
        print(f"  - {edge.id}: connects nodes {edge.nodes}")

    print(f"\nIs Acyclic: {hypergraph.is_acyclic}")

    if hypergraph.gyo_steps:
        print(f"\nGYO Steps ({len(hypergraph.gyo_steps)}):")
        for step in hypergraph.gyo_steps:
            print(f"  Step {step.step_number}: {step.action}")
            print(f"    Description: {step.description}")
            print(f"    Removed edges: {step.removed_edges}")
            print(f"    Removed nodes: {step.removed_nodes}")
            print(f"    Remaining edges: {step.remaining_edges}")

    if hypergraph.join_tree:
        print(f"\nJoin Tree ({len(hypergraph.join_tree)} nodes):")
        roots = [node for node in hypergraph.join_tree if node.parent is None]
        print(f"Number of roots: {len(roots)}")
        for root in roots:
            print(f"  Root: {root.relation} (id: {root.id})")

    print("=" * 80)

    # Assertions
    assert hypergraph.is_acyclic, (
        f"Query should be ACYCLIC but was classified as CYCLIC. "
        f"This is a tree structure with two star patterns connected through movie_link. "
        f"Nodes: {len(hypergraph.nodes)}, Edges: {len(hypergraph.edges)}. "
        f"Remaining edges after GYO: {hypergraph.gyo_steps[-1].remaining_edges if hypergraph.gyo_steps else 'N/A'}"
    )
    assert hypergraph.join_tree is not None, "Join tree should be generated for acyclic query"
    assert len(hypergraph.join_tree) > 0, "Join tree should not be empty"


def test_simplified_movie_link(spark_session):
    """Test a simplified version with fewer tables to isolate the issue"""

    query = """
    SELECT MIN(t1.title), MIN(t2.title)
    FROM title AS t1,
         title AS t2,
         movie_link AS ml,
         link_type AS lt,
         movie_companies AS mc1,
         movie_companies AS mc2
    WHERE lt.id = ml.link_type_id
      AND t1.id = ml.movie_id
      AND t2.id = ml.linked_movie_id
      AND mc1.movie_id = t1.id
      AND mc2.movie_id = t2.id
    """

    extractor = HypergraphExtractor(spark_session)
    hypergraph = extractor.extract_hypergraph(query, "test_simplified_movie_link")

    print("\n" + "=" * 80)
    print("SIMPLIFIED MOVIE LINK QUERY")
    print("=" * 80)
    print(f"Nodes: {len(hypergraph.nodes)}")
    print(f"Edges: {len(hypergraph.edges)}")
    print(f"Is Acyclic: {hypergraph.is_acyclic}")

    for node in hypergraph.nodes:
        print(f"  Node {node.id}: {node.attributes}")

    for edge in hypergraph.edges:
        print(f"  Edge {edge.id}: connects {edge.nodes}")

    print("=" * 80)

    assert hypergraph.is_acyclic, (
        f"Simplified query should be acyclic. "
        f"Structure: lt-ml-t1-mc1 and ml-t2-mc2 (tree pattern)"
    )


def test_budget_query_acyclic(spark_session):
    """
    Test the budget query - should be ACYCLIC

    This query has a star-like structure with t (title) at the center:
    - t connects to mi, mi_idx, and mc
    - mi connects to it1
    - mi_idx connects to it2
    - mc connects to ct and cn

    All paths go through t, so it's acyclic (tree structure).
    """
    query = """
    SELECT MIN(mi.info) AS budget,
           MIN(t.title) AS unsuccsessful_movie
    FROM company_name AS cn,
         company_type AS ct,
         info_type AS it1,
         info_type AS it2,
         movie_companies AS mc,
         movie_info AS mi,
         movie_info_idx AS mi_idx,
         title AS t
    WHERE cn.country_code ='[us]'
      AND ct.kind IS NOT NULL
      AND (ct.kind ='production companies'
           OR ct.kind = 'distributors')
      AND it1.info ='budget'
      AND it2.info ='bottom 10 rank'
      AND t.production_year >2000
      AND (t.title LIKE 'Birdemic%'
           OR t.title LIKE '%Movie%')
      AND t.id = mi.movie_id
      AND t.id = mi_idx.movie_id
      AND mi.info_type_id = it1.id
      AND mi_idx.info_type_id = it2.id
      AND t.id = mc.movie_id
      AND ct.id = mc.company_type_id
      AND cn.id = mc.company_id
      AND mc.movie_id = mi.movie_id
      AND mc.movie_id = mi_idx.movie_id
      AND mi.movie_id = mi_idx.movie_id
    """

    extractor = HypergraphExtractor(spark_session)
    hypergraph = extractor.extract_hypergraph(query, "test_budget_query")

    print("\n" + "=" * 80)
    print("BUDGET QUERY HYPERGRAPH STRUCTURE")
    print("=" * 80)

    print(f"\nNodes ({len(hypergraph.nodes)}):")
    for node in hypergraph.nodes:
        print(f"  - {node.id}: {node.attributes}")

    print(f"\nEdges ({len(hypergraph.edges)}):")
    for edge in hypergraph.edges:
        print(f"  - {edge.id}: connects nodes {edge.nodes}")

    print(f"\nIs Acyclic: {hypergraph.is_acyclic}")

    if hypergraph.gyo_steps:
        print(f"\nGYO Steps ({len(hypergraph.gyo_steps)}):")
        for step in hypergraph.gyo_steps:
            print(f"  Step {step.step_number}: {step.action}")
            print(f"    {step.description}")
            print(f"    Remaining edges: {len(step.remaining_edges)}")

    print("=" * 80)

    # Assertions
    assert hypergraph.is_acyclic, (
        f"Budget query should be ACYCLIC (star/tree structure with t at center). "
        f"Nodes: {len(hypergraph.nodes)}, Edges: {len(hypergraph.edges)}. "
        f"Final GYO state: {hypergraph.gyo_steps[-1].remaining_edges if hypergraph.gyo_steps else 'N/A'}"
    )
    assert hypergraph.join_tree is not None, "Join tree should be generated for acyclic query"
