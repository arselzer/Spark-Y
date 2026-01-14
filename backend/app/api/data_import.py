"""
API routes for importing data from external databases into Spark
"""
from fastapi import APIRouter, HTTPException, Response, UploadFile, File, Query
from fastapi.responses import StreamingResponse, FileResponse
from pydantic import BaseModel
from typing import List, Optional
import logging
import os
import io
import gzip
import json
import re
from pathlib import Path
from datetime import datetime

from app.dependencies import get_spark_manager

logger = logging.getLogger(__name__)
router = APIRouter()

# SQL dumps directory
SQL_DUMPS_DIR = Path("/app/data/sql-dumps")
SQL_DUMPS_DIR.mkdir(parents=True, exist_ok=True)

# FK maps directory
FK_MAPS_DIR = Path("/app/data/fk-maps")
FK_MAPS_DIR.mkdir(parents=True, exist_ok=True)


class PostgresConfig(BaseModel):
    """Configuration for PostgreSQL connection"""
    host: str = "postgres"
    port: int = 5432
    database: str = "imdb"
    username: str = "postgres"
    password: str = "postgres"


class ImportResponse(BaseModel):
    """Response from data import operation"""
    success: bool
    message: str
    tables_imported: List[str]
    errors: List[str] = []


class ForeignKeyRelationship(BaseModel):
    """A single foreign key relationship"""
    source_table: str
    source_column: str
    target_table: str
    target_column: str


class ForeignKeyMap(BaseModel):
    """A collection of foreign key relationships"""
    name: str
    description: Optional[str] = None
    relationships: List[ForeignKeyRelationship]


@router.post("/postgres", response_model=ImportResponse)
async def import_from_postgres(config: PostgresConfig):
    """
    Import data from PostgreSQL database into Spark SQL

    This will:
    1. Connect to the specified PostgreSQL database
    2. Query information_schema to get list of tables
    3. Import each table from the public schema
    4. Create temporary views in Spark for each table
    """
    try:
        spark_manager = get_spark_manager()
        spark = spark_manager.spark

        if spark is None:
            raise HTTPException(status_code=503, detail="Spark session not initialized")

        jdbc_url = f"jdbc:postgresql://{config.host}:{config.port}/{config.database}"
        logger.info(f"Connecting to PostgreSQL: {jdbc_url}")

        connection_props = {
            "user": config.username,
            "password": config.password,
            "driver": "org.postgresql.Driver"
        }

        tables_imported = []
        errors = []

        try:
            # Query information_schema to get list of tables in current database's public schema
            # Filter by: current database, public schema, and base tables only (exclude views, etc.)
            tables_query = """(
                SELECT table_name
                FROM information_schema.tables
                WHERE table_catalog = current_database()
                AND table_schema = 'public'
                AND table_type = 'BASE TABLE'
            ) as tables"""

            df_tables = spark.read.format("jdbc") \
                .option("url", jdbc_url) \
                .option("driver", "org.postgresql.Driver") \
                .option("dbtable", tables_query) \
                .option("user", config.username) \
                .option("password", config.password) \
                .load()

            tables = [row.table_name for row in df_tables.collect()]

            logger.info(f"Found {len(tables)} tables in public schema")

            # Import each table
            for table_name in tables:
                try:
                    logger.info(f"Importing table: {table_name}")

                    df = spark.read.format("jdbc") \
                        .option("url", jdbc_url) \
                        .option("driver", "org.postgresql.Driver") \
                        .option("dbtable", table_name) \
                        .option("user", config.username) \
                        .option("password", config.password) \
                        .load()

                    # Create or replace temporary view
                    df.createOrReplaceTempView(table_name)

                    row_count = df.count()
                    logger.info(f"Imported {table_name}: {row_count} rows")
                    tables_imported.append(table_name)

                except Exception as e:
                    error_msg = f"Failed to import table {table_name}: {str(e)}"
                    logger.error(error_msg)
                    errors.append(error_msg)

            # Set current database in spark_manager after successful import
            if len(tables_imported) > 0:
                spark_manager.set_current_database(config.database)

            if len(tables_imported) == 0:
                return ImportResponse(
                    success=False,
                    message="No tables were imported",
                    tables_imported=[],
                    errors=errors
                )

            return ImportResponse(
                success=True,
                message=f"Successfully imported {len(tables_imported)} tables",
                tables_imported=tables_imported,
                errors=errors
            )

        except Exception as e:
            error_msg = f"Failed to connect to PostgreSQL or query tables: {str(e)}"
            logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error during import: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/postgres/test-connection")
async def test_postgres_connection(
    host: str = "postgres",
    port: int = 5432,
    database: str = "imdb",
    username: str = "postgres",
    password: str = "postgres"
):
    """
    Test PostgreSQL connection without importing data
    """
    try:
        spark_manager = get_spark_manager()
        spark = spark_manager.spark

        if spark is None:
            raise HTTPException(status_code=503, detail="Spark session not initialized")

        jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"

        # Query public schema tables in the current database only
        # Filter by: current database, public schema, and base tables only (exclude views, etc.)
        table_count_query = """(
            SELECT COUNT(*) as cnt
            FROM information_schema.tables
            WHERE table_catalog = current_database()
            AND table_schema = 'public'
            AND table_type = 'BASE TABLE'
        ) as test"""

        df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", table_count_query) \
            .option("user", username) \
            .option("password", password) \
            .load()

        result = df.collect()[0]
        table_count = result['cnt']

        return {
            "success": True,
            "message": f"Successfully connected to PostgreSQL",
            "database": database,
            "total_tables": table_count
        }

    except Exception as e:
        logger.error(f"Connection test failed: {str(e)}")
        return {
            "success": False,
            "message": f"Connection failed: {str(e)}",
            "database": database
        }


@router.get("/imported-tables")
async def list_imported_tables():
    """
    List all tables currently available in Spark SQL
    """
    try:
        spark_manager = get_spark_manager()
        spark = spark_manager.spark

        if spark is None:
            raise HTTPException(status_code=503, detail="Spark session not initialized")

        # Get list of temporary tables/views
        tables = spark.catalog.listTables()

        table_list = [
            {
                "name": table.name,
                "database": table.database,
                "description": table.description,
                "tableType": table.tableType,
                "isTemporary": table.isTemporary
            }
            for table in tables
        ]

        return {
            "tables": table_list,
            "count": len(table_list)
        }

    except Exception as e:
        logger.error(f"Failed to list tables: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/postgres/tables/{table_name}")
async def delete_postgres_table(
    table_name: str,
    host: str = "postgres",
    port: int = 5432,
    database: str = "imdb",
    username: str = "postgres",
    password: str = "postgres"
):
    """
    Delete a table from PostgreSQL database

    Also drops the corresponding Spark temporary view if it exists
    """
    try:
        import psycopg2
    except ImportError:
        raise HTTPException(
            status_code=500,
            detail="psycopg2 not installed. Cannot delete table from PostgreSQL."
        )

    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=username,
            password=password
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Drop table from PostgreSQL
        cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
        logger.info(f"Dropped table {table_name} from PostgreSQL")

        cursor.close()
        conn.close()

        # Also drop from Spark if exists
        try:
            spark_manager = get_spark_manager()
            spark = spark_manager.spark

            if spark is not None:
                spark.sql(f"DROP TABLE IF EXISTS {table_name}")
                logger.info(f"Dropped table {table_name} from Spark")
        except Exception as e:
            logger.warning(f"Failed to drop table from Spark: {str(e)}")

        return {
            "success": True,
            "message": f"Successfully deleted table '{table_name}' from PostgreSQL and Spark",
            "table_name": table_name
        }

    except psycopg2.Error as e:
        error_msg = f"PostgreSQL error: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)
    except Exception as e:
        error_msg = f"Failed to delete table: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)


@router.post("/postgres/tables/{table_name}/columns")
async def get_table_columns(
    table_name: str,
    host: str = "postgres",
    port: int = 5432,
    database: str = "imdb",
    username: str = "postgres",
    password: str = "postgres"
):
    """
    Get column names for a specific table from PostgreSQL

    Returns a list of column names for the specified table
    """
    try:
        import psycopg2

        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=username,
            password=password
        )
        cursor = conn.cursor()

        # Query information_schema for column names
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            ORDER BY ordinal_position
        """, (table_name,))

        columns = [row[0] for row in cursor.fetchall()]

        cursor.close()
        conn.close()

        return {
            "table_name": table_name,
            "columns": columns,
            "count": len(columns)
        }

    except psycopg2.Error as e:
        error_msg = f"PostgreSQL error: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)
    except Exception as e:
        error_msg = f"Failed to get table columns: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)


# Default IMDB foreign key map (extracted from JOB benchmark queries)
DEFAULT_IMDB_FOREIGN_KEYS = {
    "movie_keyword": [
        {"fk_column": "movie_id", "ref_table": "title", "ref_column": "id"},
        {"fk_column": "keyword_id", "ref_table": "keyword", "ref_column": "id"}
    ],
    "movie_companies": [
        {"fk_column": "movie_id", "ref_table": "title", "ref_column": "id"},
        {"fk_column": "company_id", "ref_table": "company_name", "ref_column": "id"},
        {"fk_column": "company_type_id", "ref_table": "company_type", "ref_column": "id"}
    ],
    "movie_info": [
        {"fk_column": "movie_id", "ref_table": "title", "ref_column": "id"},
        {"fk_column": "info_type_id", "ref_table": "info_type", "ref_column": "id"}
    ],
    "movie_info_idx": [
        {"fk_column": "movie_id", "ref_table": "title", "ref_column": "id"},
        {"fk_column": "info_type_id", "ref_table": "info_type", "ref_column": "id"}
    ],
    "cast_info": [
        {"fk_column": "movie_id", "ref_table": "title", "ref_column": "id"},
        {"fk_column": "person_id", "ref_table": "name", "ref_column": "id"},
        {"fk_column": "person_role_id", "ref_table": "char_name", "ref_column": "id"},
        {"fk_column": "role_id", "ref_table": "role_type", "ref_column": "id"}
    ],
    "complete_cast": [
        {"fk_column": "movie_id", "ref_table": "title", "ref_column": "id"},
        {"fk_column": "subject_id", "ref_table": "comp_cast_type", "ref_column": "id"},
        {"fk_column": "status_id", "ref_table": "comp_cast_type", "ref_column": "id"}
    ],
    "movie_link": [
        {"fk_column": "movie_id", "ref_table": "title", "ref_column": "id"},
        {"fk_column": "linked_movie_id", "ref_table": "title", "ref_column": "id"},
        {"fk_column": "link_type_id", "ref_table": "link_type", "ref_column": "id"}
    ],
    "title": [
        {"fk_column": "kind_id", "ref_table": "kind_type", "ref_column": "id"}
    ],
    "aka_name": [
        {"fk_column": "person_id", "ref_table": "name", "ref_column": "id"}
    ],
    "aka_title": [
        {"fk_column": "movie_id", "ref_table": "title", "ref_column": "id"}
    ],
    "person_info": [
        {"fk_column": "person_id", "ref_table": "name", "ref_column": "id"},
        {"fk_column": "info_type_id", "ref_table": "info_type", "ref_column": "id"}
    ]
}


def _get_foreign_keys(spark, jdbc_url: str, username: str, password: str, manual_fks: Optional[dict] = None) -> dict:
    """
    Query PostgreSQL for foreign key relationships, with fallback to manual FKs.

    Args:
        spark: Spark session
        jdbc_url: JDBC connection URL
        username: Database username
        password: Database password
        manual_fks: Optional manual FK specification (same format as return value)

    Returns a dict mapping:
    {
        'table_name': [
            {
                'fk_column': 'column_name',
                'ref_table': 'referenced_table',
                'ref_column': 'referenced_column'
            },
            ...
        ]
    }
    """
    fks_by_table = {}

    # First, try to get FKs from database schema
    try:
        fk_query = """(
            SELECT
                tc.table_name,
                kcu.column_name AS fk_column,
                ccu.table_name AS ref_table,
                ccu.column_name AS ref_column
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = 'public'
            ORDER BY tc.table_name, kcu.ordinal_position
        ) as fks"""

        df_fks = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", fk_query) \
            .option("user", username) \
            .option("password", password) \
            .load()

        for row in df_fks.collect():
            table = row['table_name']
            if table not in fks_by_table:
                fks_by_table[table] = []
            fks_by_table[table].append({
                'fk_column': row['fk_column'],
                'ref_table': row['ref_table'],
                'ref_column': row['ref_column']
            })

        logger.info(f"Discovered {len(fks_by_table)} tables with FK constraints from schema")
    except Exception as e:
        logger.warning(f"Could not query FK constraints from schema: {str(e)}")

    # Merge with manual FKs (manual FKs override schema FKs)
    if manual_fks:
        logger.info(f"Applying manual FK specifications for {len(manual_fks)} tables")
        for table, fks in manual_fks.items():
            fks_by_table[table] = fks

    return fks_by_table


@router.post("/postgres/export")
async def export_to_sql_dump(
    config: PostgresConfig,
    max_rows: int = 10000,
    save_to_library: bool = False,
    smart_sampling: bool = False,
    use_imdb_fks: bool = False,
    compress: bool = False,
    tables: Optional[List[str]] = Query(None),
    custom_fk_map: Optional[str] = None
):
    """
    Export PostgreSQL database to SQL dump file

    For tables with fewer than max_rows: exports all rows
    For tables with >= max_rows: samples max_rows rows randomly

    If tables is provided, only exports the specified tables
    Otherwise exports all tables in the database

    If smart_sampling is True, uses FK-aware sampling:
    - Small tables (≤ max_rows) become "anchor" tables
    - Large tables prioritize rows with FK matches to anchors
    - Remaining slots filled with random samples
    - Creates demo DBs with realistic join results

    If use_imdb_fks is True, uses IMDB schema FK map (for databases without explicit FKs)
    The IMDB FK map is extracted from JOB benchmark queries

    If custom_fk_map is provided, uses the custom FK map instead of IMDB or schema FKs
    The custom FK map should be a JSON string with format:
    {"name": "Map Name", "relationships": [{"source_table": "t1", "source_column": "c1",
    "target_table": "t2", "target_column": "c2"}, ...]}

    If compress is True, compresses the dump using gzip (.sql.gz extension)

    If save_to_library is True, saves the dump to the SQL dumps library
    Returns a SQL dump file that can be used to recreate the database
    """
    try:
        spark_manager = get_spark_manager()
        spark = spark_manager.spark

        if spark is None:
            raise HTTPException(status_code=503, detail="Spark session not initialized")

        jdbc_url = f"jdbc:postgresql://{config.host}:{config.port}/{config.database}"
        logger.info(f"Exporting from PostgreSQL: {jdbc_url}, max_rows={max_rows}, smart_sampling={smart_sampling}")

        connection_props = {
            "user": config.username,
            "password": config.password,
            "driver": "org.postgresql.Driver"
        }

        # Build SQL dump in memory
        sql_dump = io.StringIO()

        # Write header
        sql_dump.write(f"-- SQL Dump for database: {config.database}\n")
        sql_dump.write(f"-- Generated by Yannasparkis\n")
        sql_dump.write(f"-- Max rows per table: {max_rows}\n")
        if smart_sampling:
            sql_dump.write(f"-- Smart FK-aware sampling: enabled\n")
            if use_imdb_fks:
                sql_dump.write(f"-- Using IMDB foreign key map (from JOB benchmark queries)\n")
        sql_dump.write("\n")

        try:
            # Get list of tables
            tables_query = """(
                SELECT table_name
                FROM information_schema.tables
                WHERE table_catalog = current_database()
                AND table_schema = 'public'
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            ) as tables"""

            df_tables = spark.read.format("jdbc") \
                .option("url", jdbc_url) \
                .option("driver", "org.postgresql.Driver") \
                .option("dbtable", tables_query) \
                .option("user", config.username) \
                .option("password", config.password) \
                .load()

            all_tables = [row.table_name for row in df_tables.collect()]

            # Filter tables if specific tables are requested
            if tables is not None:
                # Validate that all requested tables exist
                requested_set = set(tables)
                available_set = set(all_tables)
                missing_tables = requested_set - available_set

                if missing_tables:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Tables not found: {', '.join(missing_tables)}"
                    )

                tables_to_export = tables
                logger.info(f"Exporting {len(tables_to_export)} selected tables: {', '.join(tables_to_export)}")
            else:
                tables_to_export = all_tables
                logger.info(f"Exporting all {len(tables_to_export)} tables")

            # Get row counts for tables to export
            table_row_counts = {}
            for table_name in tables_to_export:
                count_query = f"(SELECT COUNT(*) as cnt FROM {table_name}) as count"
                df_count = spark.read.format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("driver", "org.postgresql.Driver") \
                    .option("dbtable", count_query) \
                    .option("user", config.username) \
                    .option("password", config.password) \
                    .load()
                table_row_counts[table_name] = df_count.collect()[0]['cnt']

            # If smart sampling, get FK relationships and identify anchor tables
            anchor_tables = set()
            fks_by_table = {}
            if smart_sampling:
                logger.info("Smart sampling enabled - discovering FK relationships")

                # Determine which FK map to use
                manual_fks = None
                if custom_fk_map:
                    # Parse custom FK map from JSON
                    try:
                        custom_map_data = json.loads(custom_fk_map)
                        # Convert custom FK map format to internal format
                        manual_fks = {}
                        for rel in custom_map_data.get('relationships', []):
                            source_table = rel['source_table']
                            if source_table not in manual_fks:
                                manual_fks[source_table] = []
                            manual_fks[source_table].append({
                                'fk_column': rel['source_column'],
                                'ref_table': rel['target_table'],
                                'ref_column': rel['target_column']
                            })
                        logger.info(f"Using custom FK map: {custom_map_data.get('name', 'Unnamed')} with {len(custom_map_data.get('relationships', []))} relationships")
                    except Exception as e:
                        logger.error(f"Failed to parse custom FK map: {str(e)}")
                        raise HTTPException(status_code=400, detail=f"Invalid custom FK map: {str(e)}")
                elif use_imdb_fks:
                    manual_fks = DEFAULT_IMDB_FOREIGN_KEYS
                    logger.info("Using IMDB foreign key map (from JOB benchmark queries)")

                fks_by_table = _get_foreign_keys(spark, jdbc_url, config.username, config.password, manual_fks)

                # Anchor tables are small tables (≤ max_rows)
                anchor_tables = {t for t, count in table_row_counts.items() if count <= max_rows}
                logger.info(f"Identified {len(anchor_tables)} anchor tables: {anchor_tables}")
                logger.info(f"Found {len(fks_by_table)} tables with FK relationships")

            for table_name in tables_to_export:
                try:
                    logger.info(f"Exporting table: {table_name}")

                    row_count = table_row_counts[table_name]
                    logger.info(f"Table {table_name} has {row_count} rows")

                    # Determine query based on row count and sampling strategy
                    if row_count <= max_rows:
                        # Export all rows
                        data_query = f"(SELECT * FROM {table_name}) as data"
                        exported_rows = row_count
                    elif smart_sampling and table_name in fks_by_table:
                        # Smart sampling: prioritize FK-matched rows
                        fks = fks_by_table[table_name]

                        # Build semi-join conditions for FKs to anchor tables
                        anchor_fks = [fk for fk in fks if fk['ref_table'] in anchor_tables]

                        if anchor_fks:
                            # Use FK-aware sampling
                            logger.info(f"Using FK-aware sampling for {table_name} with {len(anchor_fks)} anchor FK(s)")

                            # Build WHERE clause for semi-join
                            # Get rows that have FK matches to anchors (70% of max_rows)
                            fk_matched_limit = int(max_rows * 0.7)
                            random_limit = max_rows - fk_matched_limit

                            # Build IN clause for each FK to anchor
                            where_conditions = []
                            for fk in anchor_fks:
                                ref_table = fk['ref_table']
                                fk_col = fk['fk_column']
                                ref_col = fk['ref_column']
                                where_conditions.append(
                                    f"{fk_col} IN (SELECT {ref_col} FROM {ref_table})"
                                )

                            where_clause = " OR ".join(where_conditions)

                            # Combine FK-matched rows with random samples using UNION
                            # This ensures we get both matching rows and diversity
                            data_query = f"""(
                                SELECT * FROM (
                                    SELECT DISTINCT * FROM {table_name}
                                    WHERE {where_clause}
                                    LIMIT {fk_matched_limit}
                                ) fk_matched
                                UNION
                                SELECT * FROM (
                                    SELECT * FROM {table_name}
                                    ORDER BY RANDOM()
                                    LIMIT {random_limit}
                                ) random_sample
                                LIMIT {max_rows}
                            ) as data"""
                            exported_rows = max_rows
                        else:
                            # No FKs to anchors, fall back to random sampling
                            logger.info(f"No FKs to anchor tables for {table_name}, using random sampling")
                            data_query = f"(SELECT * FROM {table_name} ORDER BY RANDOM() LIMIT {max_rows}) as data"
                            exported_rows = max_rows
                    else:
                        # Regular random sampling
                        data_query = f"(SELECT * FROM {table_name} ORDER BY RANDOM() LIMIT {max_rows}) as data"
                        exported_rows = max_rows

                    # Read data
                    df_data = spark.read.format("jdbc") \
                        .option("url", jdbc_url) \
                        .option("driver", "org.postgresql.Driver") \
                        .option("dbtable", data_query) \
                        .option("user", config.username) \
                        .option("password", config.password) \
                        .load()

                    rows = df_data.collect()

                    if len(rows) == 0:
                        logger.info(f"Table {table_name} is empty, skipping")
                        continue

                    # Write table header
                    sql_dump.write(f"\n-- Table: {table_name} ({exported_rows}/{row_count} rows)\n")

                    # Get column names and types
                    columns = df_data.columns

                    # Write CREATE TABLE statement (simplified - just column names)
                    sql_dump.write(f"DROP TABLE IF EXISTS {table_name} CASCADE;\n")
                    sql_dump.write(f"CREATE TABLE {table_name} (\n")

                    # Get schema from information_schema
                    schema_query = f"""(
                        SELECT column_name, data_type, character_maximum_length, is_nullable
                        FROM information_schema.columns
                        WHERE table_schema = 'public'
                        AND table_name = '{table_name}'
                        ORDER BY ordinal_position
                    ) as schema"""

                    df_schema = spark.read.format("jdbc") \
                        .option("url", jdbc_url) \
                        .option("driver", "org.postgresql.Driver") \
                        .option("dbtable", schema_query) \
                        .option("user", config.username) \
                        .option("password", config.password) \
                        .load()

                    schema_rows = df_schema.collect()
                    column_defs = []
                    for col in schema_rows:
                        col_name = col['column_name']
                        data_type = col['data_type']
                        max_length = col['character_maximum_length']
                        nullable = col['is_nullable']

                        # Build column definition
                        col_def = f"  {col_name} {data_type.upper()}"
                        if max_length and data_type in ['character varying', 'character']:
                            col_def += f"({max_length})"
                        if nullable == 'NO':
                            col_def += " NOT NULL"
                        column_defs.append(col_def)

                    sql_dump.write(",\n".join(column_defs))
                    sql_dump.write("\n);\n\n")

                    # Write INSERT statements in batches for better performance
                    batch_size = 1000
                    for batch_start in range(0, len(rows), batch_size):
                        batch_rows = rows[batch_start:batch_start + batch_size]

                        # Build multi-value INSERT statement
                        sql_dump.write(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES\n")

                        value_rows = []
                        for row in batch_rows:
                            values = []
                            for col_name in columns:
                                val = row[col_name]
                                if val is None:
                                    values.append("NULL")
                                elif isinstance(val, str):
                                    # Escape single quotes
                                    escaped = val.replace("'", "''")
                                    values.append(f"'{escaped}'")
                                elif isinstance(val, (int, float)):
                                    values.append(str(val))
                                elif isinstance(val, bool):
                                    values.append("TRUE" if val else "FALSE")
                                else:
                                    # Convert to string and escape
                                    escaped = str(val).replace("'", "''")
                                    values.append(f"'{escaped}'")

                            value_rows.append(f"  ({', '.join(values)})")

                        sql_dump.write(",\n".join(value_rows))
                        sql_dump.write(";\n")

                    sql_dump.write("\n")
                    logger.info(f"Exported {len(rows)} rows from {table_name} in {(len(rows) + batch_size - 1) // batch_size} batch(es)")

                except Exception as e:
                    error_msg = f"Failed to export table {table_name}: {str(e)}"
                    logger.error(error_msg)
                    sql_dump.write(f"\n-- ERROR: {error_msg}\n\n")

            # Return as downloadable file
            sql_content = sql_dump.getvalue()
            sql_dump.close()

            # Optionally compress the dump
            if compress:
                # Compress using gzip
                compressed_data = gzip.compress(sql_content.encode('utf-8'))
                file_extension = "sql.gz"
                media_type = "application/gzip"
                content = compressed_data
                logger.info(f"Compressed SQL dump: {len(sql_content)} bytes -> {len(compressed_data)} bytes (ratio: {len(compressed_data)/len(sql_content):.2%})")
            else:
                file_extension = "sql"
                media_type = "application/sql"
                content = sql_content

            # Optionally save to SQL dumps library
            if save_to_library:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{config.database}_{timestamp}.{file_extension}"
                filepath = SQL_DUMPS_DIR / filename

                if compress:
                    with open(filepath, 'wb') as f:
                        f.write(content)
                else:
                    with open(filepath, 'w') as f:
                        f.write(content)

                logger.info(f"Saved SQL dump to library: {filename}")

            return Response(
                content=content,
                media_type=media_type,
                headers={
                    "Content-Disposition": f"attachment; filename={config.database}_export.{file_extension}"
                }
            )

        except Exception as e:
            error_msg = f"Failed to export database: {str(e)}"
            logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error during export: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/postgres/import-sql")
async def import_sql_dump(
    file: UploadFile = File(...),
    host: str = "postgres",
    port: int = 5432,
    database: str = "imdb",
    username: str = "postgres",
    password: str = "postgres"
):
    """
    Import SQL dump file into PostgreSQL database

    Supports both uncompressed (.sql) and gzip-compressed (.sql.gz) files
    Executes SQL statements against PostgreSQL, then uses the existing
    PostgreSQL-to-Spark import logic to load tables into Spark.
    """
    try:
        # Read file contents
        contents = await file.read()

        # Check if file is gzip-compressed
        is_compressed = file.filename.endswith('.gz') if file.filename else False

        if is_compressed:
            # Decompress gzip file
            sql_content = gzip.decompress(contents).decode('utf-8')
            logger.info(f"Importing compressed SQL dump: {file.filename}, compressed: {len(contents)} bytes, uncompressed: {len(sql_content)} bytes")
        else:
            sql_content = contents.decode('utf-8')
            logger.info(f"Importing SQL dump file: {file.filename}, size: {len(sql_content)} bytes")

        # Import using psycopg2 for direct PostgreSQL execution
        try:
            import psycopg2
        except ImportError:
            raise HTTPException(
                status_code=500,
                detail="psycopg2 not installed. Cannot execute SQL dump directly."
            )

        executed_statements = 0
        errors = []

        try:
            # Connect to PostgreSQL
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=username,
                password=password
            )
            # Use transaction instead of autocommit for better performance
            conn.autocommit = False
            cursor = conn.cursor()

            logger.info("Starting SQL import with transaction batching...")

            # Split SQL into individual statements
            statements = re.split(r';\s*\n', sql_content)

            # Process statements in transaction batches
            transaction_batch_size = 1000
            batch_count = 0

            for statement in statements:
                statement = statement.strip()

                # Skip empty statements and comments
                if not statement or statement.startswith('--'):
                    continue

                try:
                    cursor.execute(statement)
                    executed_statements += 1

                    # Commit every N statements for progress checkpointing
                    if executed_statements % transaction_batch_size == 0:
                        conn.commit()
                        batch_count += 1
                        logger.info(f"Executed {executed_statements} statements ({batch_count} transaction batches)...")

                except Exception as e:
                    error_msg = f"Error executing statement (truncated): {statement[:100]}... Error: {str(e)}"
                    logger.warning(error_msg)
                    errors.append(error_msg)
                    # Continue with next statement (don't rollback entire transaction)

            # Commit remaining statements
            conn.commit()
            logger.info(f"Committed final transaction batch")

            cursor.close()
            conn.close()

            logger.info(f"Executed {executed_statements} SQL statements in PostgreSQL using {batch_count + 1} transaction batch(es)")

            # Now use existing import logic to load tables into Spark
            config = PostgresConfig(
                host=host,
                port=port,
                database=database,
                username=username,
                password=password
            )

            # Call the existing import function
            import_result = await import_from_postgres(config)

            return {
                "success": True,
                "message": f"Successfully executed {executed_statements} SQL statements in {batch_count + 1} transaction batch(es) and imported {len(import_result.tables_imported)} tables to Spark",
                "executed_statements": executed_statements,
                "transaction_batches": batch_count + 1,
                "tables_imported_to_spark": len(import_result.tables_imported),
                "errors": errors[:10] + import_result.errors[:10]  # Return first 10 of each
            }

        except psycopg2.Error as e:
            error_msg = f"PostgreSQL error: {str(e)}"
            logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error during SQL import: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sql-dumps")
async def list_sql_dumps():
    """
    List all SQL dump files in the library

    Includes both uncompressed (.sql) and gzip-compressed (.sql.gz) files
    """
    try:
        dumps = []

        if SQL_DUMPS_DIR.exists():
            # Collect both .sql and .sql.gz files
            all_files = list(SQL_DUMPS_DIR.glob("*.sql")) + list(SQL_DUMPS_DIR.glob("*.sql.gz"))

            for filepath in sorted(all_files, key=lambda p: p.stat().st_mtime, reverse=True):
                stat = filepath.stat()
                is_compressed = filepath.name.endswith('.gz')

                dump_info = {
                    "filename": filepath.name,
                    "size": stat.st_size,
                    "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                    "compressed": is_compressed
                }

                dumps.append(dump_info)

        return {
            "dumps": dumps,
            "count": len(dumps)
        }
    except Exception as e:
        logger.error(f"Failed to list SQL dumps: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sql-dumps/{filename}")
async def download_sql_dump(filename: str):
    """
    Download a SQL dump file from the library
    """
    try:
        filepath = SQL_DUMPS_DIR / filename

        # Security: prevent directory traversal
        if not filepath.resolve().is_relative_to(SQL_DUMPS_DIR.resolve()):
            raise HTTPException(status_code=403, detail="Access denied")

        if not filepath.exists():
            raise HTTPException(status_code=404, detail="SQL dump not found")

        return FileResponse(
            path=filepath,
            media_type="application/sql",
            filename=filename
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to download SQL dump: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sql-dumps/{filename}/metadata")
async def get_sql_dump_metadata(filename: str):
    """
    Extract metadata from a SQL dump file without loading it

    Supports both uncompressed (.sql) and gzip-compressed (.sql.gz) files

    Returns:
    - File information (size, modified date, compressed status)
    - Table names found in the dump
    - Estimated row counts per table
    - Total statement count
    """
    try:
        filepath = SQL_DUMPS_DIR / filename

        # Security: prevent directory traversal
        if not filepath.resolve().is_relative_to(SQL_DUMPS_DIR.resolve()):
            raise HTTPException(status_code=403, detail="Access denied")

        if not filepath.exists():
            raise HTTPException(status_code=404, detail="SQL dump not found")

        # Get file metadata
        stat = filepath.stat()
        is_compressed = filename.endswith('.gz')

        # Read and parse SQL content
        if is_compressed:
            with open(filepath, 'rb') as f:
                sql_content = gzip.decompress(f.read()).decode('utf-8')
            uncompressed_size = len(sql_content.encode('utf-8'))
            file_info = {
                "filename": filename,
                "size": stat.st_size,
                "size_mb": round(stat.st_size / (1024 * 1024), 2),
                "uncompressed_size": uncompressed_size,
                "uncompressed_size_mb": round(uncompressed_size / (1024 * 1024), 2),
                "compression_ratio": round(stat.st_size / uncompressed_size, 3) if uncompressed_size > 0 else 1.0,
                "compressed": True,
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
            }
        else:
            sql_content = filepath.read_text()
            file_info = {
                "filename": filename,
                "size": stat.st_size,
                "size_mb": round(stat.st_size / (1024 * 1024), 2),
                "compressed": False,
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
            }

        # Split into statements
        statements = re.split(r';\s*\n', sql_content)

        # Track tables and their row counts
        table_info = {}
        create_table_pattern = re.compile(r'CREATE TABLE (?:IF NOT EXISTS )?([^\s(]+)', re.IGNORECASE)
        insert_pattern = re.compile(r'INSERT INTO ([^\s(]+)', re.IGNORECASE)

        total_statements = 0

        for statement in statements:
            statement = statement.strip()

            # Skip empty statements and comments
            if not statement or statement.startswith('--'):
                continue

            total_statements += 1

            # Check for CREATE TABLE
            create_match = create_table_pattern.search(statement)
            if create_match:
                table_name = create_match.group(1)
                if table_name not in table_info:
                    table_info[table_name] = {
                        "name": table_name,
                        "row_count": 0,
                        "has_create_statement": True
                    }
                else:
                    table_info[table_name]["has_create_statement"] = True

            # Check for INSERT INTO
            insert_match = insert_pattern.search(statement)
            if insert_match:
                table_name = insert_match.group(1)
                if table_name not in table_info:
                    table_info[table_name] = {
                        "name": table_name,
                        "row_count": 0,
                        "has_create_statement": False
                    }

                # Count rows in this INSERT statement
                # Multi-value INSERTs have multiple sets of values
                # Count the number of closing parentheses after VALUES
                values_section = statement[statement.upper().find('VALUES'):]
                row_count = values_section.count('),') + 1  # +1 for the last row
                table_info[table_name]["row_count"] += row_count

        # Convert table_info dict to sorted list
        tables = sorted(table_info.values(), key=lambda t: t["name"])

        # Calculate totals
        total_rows = sum(t["row_count"] for t in tables)

        return {
            "file": file_info,
            "tables": tables,
            "summary": {
                "total_tables": len(tables),
                "total_rows": total_rows,
                "total_statements": total_statements
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to extract metadata from SQL dump: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sql-dumps/{filename}/load")
async def load_sql_dump_from_library(
    filename: str,
    host: str = "postgres",
    port: int = 5432,
    database: str = "imdb",
    username: str = "postgres",
    password: str = "postgres"
):
    """
    Load a SQL dump from the library into PostgreSQL

    Supports both uncompressed (.sql) and gzip-compressed (.sql.gz) files
    Reads the dump file, executes it against PostgreSQL, then imports tables to Spark
    """
    try:
        filepath = SQL_DUMPS_DIR / filename

        # Security: prevent directory traversal
        if not filepath.resolve().is_relative_to(SQL_DUMPS_DIR.resolve()):
            raise HTTPException(status_code=403, detail="Access denied")

        if not filepath.exists():
            raise HTTPException(status_code=404, detail="SQL dump not found")

        # Read file contents
        is_compressed = filename.endswith('.gz')

        if is_compressed:
            with open(filepath, 'rb') as f:
                compressed_data = f.read()
                sql_content = gzip.decompress(compressed_data).decode('utf-8')
            logger.info(f"Loading compressed SQL dump from library: {filename}, compressed: {len(compressed_data)} bytes, uncompressed: {len(sql_content)} bytes")
        else:
            sql_content = filepath.read_text()
            logger.info(f"Loading SQL dump from library: {filename}, size: {len(sql_content)} bytes")

        # Import using psycopg2 for direct PostgreSQL execution
        try:
            import psycopg2
        except ImportError:
            raise HTTPException(
                status_code=500,
                detail="psycopg2 not installed. Cannot execute SQL dump directly."
            )

        executed_statements = 0
        errors = []

        try:
            # Connect to PostgreSQL
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=username,
                password=password
            )
            # Use transaction instead of autocommit for better performance
            conn.autocommit = False
            cursor = conn.cursor()

            logger.info("Starting SQL import with transaction batching...")

            # Split SQL into individual statements
            statements = re.split(r';\s*\n', sql_content)

            # Process statements in transaction batches
            transaction_batch_size = 1000
            batch_count = 0

            for statement in statements:
                statement = statement.strip()

                # Skip empty statements and comments
                if not statement or statement.startswith('--'):
                    continue

                try:
                    cursor.execute(statement)
                    executed_statements += 1

                    # Commit every N statements for progress checkpointing
                    if executed_statements % transaction_batch_size == 0:
                        conn.commit()
                        batch_count += 1
                        logger.info(f"Executed {executed_statements} statements ({batch_count} transaction batches)...")

                except Exception as e:
                    error_msg = f"Error executing statement (truncated): {statement[:100]}... Error: {str(e)}"
                    logger.warning(error_msg)
                    errors.append(error_msg)
                    # Continue with next statement (don't rollback entire transaction)

            # Commit remaining statements
            conn.commit()
            logger.info(f"Committed final transaction batch")

            cursor.close()
            conn.close()

            logger.info(f"Executed {executed_statements} SQL statements in PostgreSQL using {batch_count + 1} transaction batch(es)")

            # Now use existing import logic to load tables into Spark
            config = PostgresConfig(
                host=host,
                port=port,
                database=database,
                username=username,
                password=password
            )

            # Call the existing import function
            import_result = await import_from_postgres(config)

            return {
                "success": True,
                "message": f"Successfully loaded {filename} and imported {len(import_result.tables_imported)} tables to Spark",
                "executed_statements": executed_statements,
                "tables_imported_to_spark": len(import_result.tables_imported),
                "errors": errors[:10] + import_result.errors[:10]  # Return first 10 of each
            }

        except psycopg2.Error as e:
            error_msg = f"PostgreSQL error: {str(e)}"
            logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error during SQL dump load: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Foreign Key Map Management Endpoints
# ============================================================================

@router.get("/fk-maps")
async def list_fk_maps():
    """
    List all custom FK maps in the library

    Returns a list of available FK maps with their metadata
    """
    try:
        maps = []

        if FK_MAPS_DIR.exists():
            for filepath in sorted(FK_MAPS_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
                try:
                    # Read the FK map file
                    with open(filepath, 'r') as f:
                        fk_data = json.load(f)

                    stat = filepath.stat()
                    maps.append({
                        "filename": filepath.name,
                        "name": fk_data.get("name", filepath.stem),
                        "description": fk_data.get("description", ""),
                        "relationship_count": len(fk_data.get("relationships", [])),
                        "size": stat.st_size,
                        "modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
                    })
                except Exception as e:
                    logger.warning(f"Failed to read FK map {filepath.name}: {str(e)}")
                    continue

        return {
            "maps": maps,
            "count": len(maps)
        }
    except Exception as e:
        logger.error(f"Failed to list FK maps: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/fk-maps")
async def save_fk_map(fk_map: ForeignKeyMap):
    """
    Save a custom FK map to the library

    The FK map is saved as a JSON file named {name}.json
    """
    try:
        # Sanitize filename (remove special characters)
        safe_name = re.sub(r'[^\w\s-]', '', fk_map.name).strip().replace(' ', '_')
        filename = f"{safe_name}.json"
        filepath = FK_MAPS_DIR / filename

        # Convert to dict
        fk_data = fk_map.model_dump()

        # Save to file
        with open(filepath, 'w') as f:
            json.dump(fk_data, f, indent=2)

        logger.info(f"Saved FK map to library: {filename}")

        return {
            "success": True,
            "message": f"FK map '{fk_map.name}' saved successfully",
            "filename": filename,
            "relationship_count": len(fk_map.relationships)
        }
    except Exception as e:
        logger.error(f"Failed to save FK map: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fk-maps/{filename}")
async def load_fk_map(filename: str):
    """
    Load a custom FK map from the library

    Returns the FK map data including all relationships
    """
    try:
        filepath = FK_MAPS_DIR / filename

        # Security: prevent directory traversal
        if not filepath.resolve().is_relative_to(FK_MAPS_DIR.resolve()):
            raise HTTPException(status_code=403, detail="Access denied")

        if not filepath.exists():
            raise HTTPException(status_code=404, detail="FK map not found")

        # Read the FK map file
        with open(filepath, 'r') as f:
            fk_data = json.load(f)

        return fk_data
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to load FK map: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/fk-maps/{filename}")
async def delete_fk_map(filename: str):
    """
    Delete a custom FK map from the library
    """
    try:
        filepath = FK_MAPS_DIR / filename

        # Security: prevent directory traversal
        if not filepath.resolve().is_relative_to(FK_MAPS_DIR.resolve()):
            raise HTTPException(status_code=403, detail="Access denied")

        if not filepath.exists():
            raise HTTPException(status_code=404, detail="FK map not found")

        # Delete the file
        filepath.unlink()

        logger.info(f"Deleted FK map from library: {filename}")

        return {
            "success": True,
            "message": f"FK map '{filename}' deleted successfully"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete FK map: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/postgres/databases")
async def list_databases(
    host: str = "postgres",
    port: int = 5432,
    username: str = "postgres",
    password: str = "postgres"
):
    """
    List all databases in the PostgreSQL instance

    Requires admin credentials (postgres user) to list all databases
    """
    try:
        import psycopg2

        # Connect to the default postgres database
        conn = psycopg2.connect(
            host=host,
            port=port,
            database="postgres",  # Connect to default postgres DB
            user=username,
            password=password
        )

        cursor = conn.cursor()

        # Query for all databases (excluding templates and postgres system databases)
        cursor.execute("""
            SELECT
                datname,
                pg_catalog.pg_get_userbyid(datdba) as owner,
                pg_encoding_to_char(encoding) as encoding,
                pg_size_pretty(pg_database_size(datname)) as size
            FROM pg_database
            WHERE datistemplate = false
              AND datname != 'postgres'
            ORDER BY datname
        """)

        databases = []
        for row in cursor.fetchall():
            databases.append({
                "name": row[0],
                "owner": row[1],
                "encoding": row[2],
                "size": row[3]
            })

        cursor.close()
        conn.close()

        logger.info(f"Listed {len(databases)} databases")

        return {
            "success": True,
            "databases": databases,
            "count": len(databases)
        }
    except Exception as e:
        logger.error(f"Failed to list databases: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/postgres/databases/create")
async def create_database(
    database_name: str,
    owner: str = "imdb",
    host: str = "postgres",
    port: int = 5432,
    username: str = "postgres",
    password: str = "postgres"
):
    """
    Create a new database in PostgreSQL

    Requires admin credentials (postgres user) to create databases
    """
    try:
        import psycopg2
        from psycopg2 import sql

        # Validate database name (alphanumeric and underscores only)
        if not database_name.replace('_', '').isalnum():
            raise HTTPException(status_code=400, detail="Database name must contain only alphanumeric characters and underscores")

        # Connect to the default postgres database
        conn = psycopg2.connect(
            host=host,
            port=port,
            database="postgres",
            user=username,
            password=password
        )

        # Must set autocommit to create database
        conn.autocommit = True
        cursor = conn.cursor()

        # Check if database already exists
        cursor.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (database_name,)
        )

        if cursor.fetchone():
            cursor.close()
            conn.close()
            raise HTTPException(status_code=409, detail=f"Database '{database_name}' already exists")

        # Create the database
        cursor.execute(
            sql.SQL("CREATE DATABASE {} OWNER {}").format(
                sql.Identifier(database_name),
                sql.Identifier(owner)
            )
        )

        logger.info(f"Created database: {database_name} (owner: {owner})")

        cursor.close()
        conn.close()

        return {
            "success": True,
            "message": f"Database '{database_name}' created successfully",
            "database": database_name,
            "owner": owner
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create database: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
