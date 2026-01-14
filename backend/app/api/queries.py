"""
API routes for query management and browsing
"""
from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
import logging
from pathlib import Path
import json

from app.models.query import QueryMetadata, QueryCategory

logger = logging.getLogger(__name__)
router = APIRouter()

# In-memory cache of queries (loaded from files)
_query_cache: Optional[List[QueryMetadata]] = None


def load_queries(force_reload: bool = False) -> List[QueryMetadata]:
    """Load queries from benchmark directory

    Args:
        force_reload: If True, clear cache and reload from disk
    """
    global _query_cache

    if force_reload:
        _query_cache = None

    if _query_cache is not None:
        return _query_cache

    queries = []

    # Load JOB queries (moved out of submodule with movie_info_idx -> movie_info rewrite)
    job_dir = Path("/app/data/job")
    if job_dir.exists():
        queries.extend(_load_job_queries(job_dir))
    else:
        logger.warning(f"JOB queries directory not found: {job_dir}")

    # Load TPC-H queries (if available)
    tpch_dir = Path("/app/spark-eval-groupagg/benchmark/tpch")
    if tpch_dir.exists():
        queries.extend(_load_tpch_queries(tpch_dir))

    _query_cache = queries
    logger.info(f"Loaded {len(queries)} queries")
    return queries


def _load_job_queries(job_dir: Path) -> List[QueryMetadata]:
    """Load JOB benchmark queries"""
    queries = []

    # Look for .sql files
    for sql_file in job_dir.glob("*.sql"):
        try:
            sql_content = sql_file.read_text()

            # Parse query metadata from filename and content
            query_id = sql_file.stem
            name = query_id.replace("_", " ").title()

            # Extract table names (simple parsing)
            tables = _extract_table_names(sql_content)

            # Count joins and aggregates
            num_joins = _count_joins(sql_content, tables)
            num_aggregates = _count_aggregates(sql_content)

            queries.append(QueryMetadata(
                query_id=f"job_{query_id}",
                name=f"JOB {name}",
                category=QueryCategory.JOB,
                sql=sql_content,
                tables=tables,
                num_joins=num_joins,
                num_aggregates=num_aggregates
            ))
        except Exception as e:
            logger.warning(f"Could not load query {sql_file}: {e}")

    return queries


def _load_tpch_queries(tpch_dir: Path) -> List[QueryMetadata]:
    """Load TPC-H benchmark queries"""
    queries = []

    for sql_file in tpch_dir.glob("*.sql"):
        try:
            sql_content = sql_file.read_text()
            query_id = sql_file.stem

            tables = _extract_table_names(sql_content)
            num_joins = _count_joins(sql_content, tables)
            num_aggregates = _count_aggregates(sql_content)

            queries.append(QueryMetadata(
                query_id=f"tpch_{query_id}",
                name=f"TPC-H {query_id}",
                category=QueryCategory.TPCH,
                sql=sql_content,
                tables=tables,
                num_joins=num_joins,
                num_aggregates=num_aggregates
            ))
        except Exception as e:
            logger.warning(f"Could not load query {sql_file}: {e}")

    return queries


def _extract_table_names(sql: str) -> List[str]:
    """Extract table names from SQL (improved parsing for comma-separated and aliased tables)"""
    import re

    tables = set()

    # Extract FROM clause with comma-separated tables
    # Pattern: FROM table1 AS alias1, table2 AS alias2, ... WHERE
    from_match = re.search(r'FROM\s+(.*?)(?:WHERE|GROUP|ORDER|LIMIT|$)', sql, re.IGNORECASE | re.DOTALL)
    if from_match:
        from_clause = from_match.group(1)
        # Split by comma to handle comma-separated tables
        table_specs = from_clause.split(',')
        for spec in table_specs:
            # Extract table name (before AS or alias)
            # Pattern: table_name AS alias or just table_name
            table_match = re.search(r'^\s*([a-zA-Z_][a-zA-Z0-9_]*)', spec.strip())
            if table_match:
                table_name = table_match.group(1)
                # Skip SQL keywords
                if table_name.upper() not in {'SELECT', 'WHERE', 'ORDER', 'GROUP', 'HAVING', 'LIMIT', 'AS', 'ON', 'AND', 'OR'}:
                    tables.add(table_name)

    # Also look for explicit JOIN keywords
    join_pattern = r'(?:JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)'
    join_matches = re.findall(join_pattern, sql, re.IGNORECASE)
    for match in join_matches:
        if match.upper() not in {'SELECT', 'WHERE', 'ORDER', 'GROUP', 'HAVING', 'LIMIT', 'AS', 'ON', 'AND', 'OR'}:
            tables.add(match)

    return sorted(list(tables))


def _count_joins(sql: str, tables: List[str]) -> int:
    """
    Count number of joins in SQL query.

    For queries with N tables, there are N-1 joins (assuming connected join graph).
    This counts both explicit JOINs and implicit comma-separated joins.
    """
    import re

    sql_upper = sql.upper()

    # Count explicit JOIN keywords
    explicit_joins = len(re.findall(r'\bJOIN\b', sql_upper))

    if explicit_joins > 0:
        # If there are explicit JOINs, count them
        return explicit_joins
    elif len(tables) > 1:
        # For comma-separated (implicit) joins, the number of joins is tables - 1
        # This assumes a connected join graph (all tables are joined together)
        return len(tables) - 1
    else:
        # Single table, no joins
        return 0


def _count_aggregates(sql: str) -> int:
    """Count number of aggregate operations in SQL query"""
    import re

    sql_upper = sql.upper()

    # Count aggregate functions
    aggregates = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']
    count = 0

    for agg in aggregates:
        # Look for function calls like COUNT(, SUM(, etc.
        pattern = rf'\b{agg}\s*\('
        matches = re.findall(pattern, sql_upper)
        count += len(matches)

    # Also check for GROUP BY presence (indicates aggregation)
    if 'GROUP BY' in sql_upper and count == 0:
        count = 1

    return count


@router.get("/", response_model=List[QueryMetadata])
async def list_queries(
    category: Optional[QueryCategory] = None,
    search: Optional[str] = None,
    limit: int = Query(default=100, le=1000),
    offset: int = Query(default=0, ge=0)
):
    """
    List available queries with optional filtering

    - **category**: Filter by benchmark category (job, tpch, etc.)
    - **search**: Search in query name, ID, or SQL content
    - **limit**: Maximum number of queries to return
    - **offset**: Number of queries to skip (pagination)
    """
    queries = load_queries()

    # Apply filters
    if category:
        queries = [q for q in queries if q.category == category]

    if search:
        search_lower = search.lower()
        queries = [
            q for q in queries
            if search_lower in q.query_id.lower()
            or search_lower in q.name.lower()
            or search_lower in q.sql.lower()
        ]

    # Apply pagination
    total = len(queries)
    queries = queries[offset:offset + limit]

    logger.info(f"Returning {len(queries)} queries (total: {total})")
    return queries


@router.get("/categories")
async def list_categories():
    """List available query categories with counts"""
    queries = load_queries()

    categories = {}
    for query in queries:
        cat = query.category.value
        if cat not in categories:
            categories[cat] = {"count": 0, "name": cat.upper()}
        categories[cat]["count"] += 1

    return categories


@router.get("/{query_id}", response_model=QueryMetadata)
async def get_query(query_id: str):
    """Get a specific query by ID"""
    queries = load_queries()

    for query in queries:
        if query.query_id == query_id:
            return query

    raise HTTPException(status_code=404, detail=f"Query not found: {query_id}")


@router.post("/reload")
async def reload_queries():
    """Reload query catalog from disk (clears cache)"""
    global _query_cache
    _query_cache = None
    queries = load_queries(force_reload=True)
    logger.info(f"Reloaded {len(queries)} queries from disk")
    return {"message": f"Successfully reloaded {len(queries)} queries", "count": len(queries)}


@router.get("/{query_id}/similar")
async def get_similar_queries(query_id: str, limit: int = 5):
    """Find similar queries based on structure"""
    queries = load_queries()

    # Find the target query
    target = None
    for q in queries:
        if q.query_id == query_id:
            target = q
            break

    if not target:
        raise HTTPException(status_code=404, detail=f"Query not found: {query_id}")

    # Simple similarity: number of joins and aggregates
    similar = []
    for q in queries:
        if q.query_id == query_id:
            continue

        # Calculate similarity score
        join_diff = abs(q.num_joins - target.num_joins)
        agg_diff = abs(q.num_aggregates - target.num_aggregates)
        table_overlap = len(set(q.tables) & set(target.tables))

        score = table_overlap * 10 - join_diff - agg_diff

        similar.append((score, q))

    # Sort by score and return top N
    similar.sort(key=lambda x: x[0], reverse=True)
    return [q for _, q in similar[:limit]]
