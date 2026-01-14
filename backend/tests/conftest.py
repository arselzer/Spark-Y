"""
Pytest configuration and fixtures for backend tests
"""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    """
    Create a SparkSession for testing

    Uses local mode with minimal resources for faster tests.
    """
    spark = (
        SparkSession.builder
        .appName("HypergraphTests")
        .master("local[1]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    yield spark

    spark.stop()


@pytest.fixture(scope="session")
def sample_data(spark_session):
    """
    Create sample tables for testing queries

    Creates simple test data mimicking the IMDB schema used in benchmarks.
    """
    spark = spark_session

    # Create sample company_name table
    company_data = [
        (1, "Universal Pictures", "us"),
        (2, "Warner Bros", "us"),
        (3, "Paramount", "us"),
    ]
    spark.createDataFrame(company_data, ["id", "name", "country_code"]).createOrReplaceTempView("company_name")

    # Create sample title table
    title_data = [
        (1, "Movie A", 2020),
        (2, "Movie B", 2021),
        (3, "Movie C", 2022),
    ]
    spark.createDataFrame(title_data, ["id", "title", "production_year"]).createOrReplaceTempView("title")

    # Create sample movie_companies table
    movie_companies_data = [
        (1, 1, 1),  # Movie 1, Company 1
        (2, 2, 2),  # Movie 2, Company 2
        (3, 3, 3),  # Movie 3, Company 3
    ]
    spark.createDataFrame(movie_companies_data, ["id", "movie_id", "company_id"]).createOrReplaceTempView("movie_companies")

    # Create sample keyword table
    keyword_data = [
        (1, "action"),
        (2, "comedy"),
        (3, "drama"),
    ]
    spark.createDataFrame(keyword_data, ["id", "keyword"]).createOrReplaceTempView("keyword")

    # Create sample movie_keyword table
    movie_keyword_data = [
        (1, 1, 1),  # Movie 1, Keyword 1
        (2, 2, 2),  # Movie 2, Keyword 2
        (3, 3, 3),  # Movie 3, Keyword 3
    ]
    spark.createDataFrame(movie_keyword_data, ["id", "movie_id", "keyword_id"]).createOrReplaceTempView("movie_keyword")

    return spark


@pytest.fixture(scope="session")
def imdb_spark(spark_session):
    """
    Create comprehensive IMDB schema tables for testing complex queries

    Includes all tables needed for JOB-style benchmark queries.
    """
    spark = spark_session

    # Create company_name table
    company_name_data = [
        (1, "Universal Pictures", "[us]"),
        (2, "Warner Bros", "[uk]"),
    ]
    spark.createDataFrame(company_name_data, ["id", "name", "country_code"]).createOrReplaceTempView("company_name")

    # Create company_type table
    company_type_data = [
        (1, "production companies"),
        (2, "distributors"),
    ]
    spark.createDataFrame(company_type_data, ["id", "kind"]).createOrReplaceTempView("company_type")

    # Create info_type table
    info_type_data = [
        (1, "budget"),
        (2, "bottom 10 rank"),
    ]
    spark.createDataFrame(info_type_data, ["id", "info"]).createOrReplaceTempView("info_type")

    # Create title table
    title_data = [
        (1, "Birdemic", 2010),
        (2, "The Room", 2003),
    ]
    spark.createDataFrame(title_data, ["id", "title", "production_year"]).createOrReplaceTempView("title")

    # Create movie_companies table
    movie_companies_data = [
        (1, 1, 1, 1, "produced by Universal"),  # id, company_id, movie_id, company_type_id, note
        (2, 2, 2, 2, "distributed by Warner Bros"),
    ]
    spark.createDataFrame(movie_companies_data, ["id", "company_id", "movie_id", "company_type_id", "note"]).createOrReplaceTempView("movie_companies")

    # Create movie_info table (used for both mi and mi_idx aliases)
    movie_info_data = [
        (1, 1, 1, "1000000"),  # id, movie_id, info_type_id, info
        (2, 1, 2, "10"),
        (3, 2, 1, "5000000"),
        (4, 2, 2, "5"),
    ]
    spark.createDataFrame(movie_info_data, ["id", "movie_id", "info_type_id", "info"]).createOrReplaceTempView("movie_info")

    return spark
