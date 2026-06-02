"""
Query Optimization Demo: Avoiding Materialization for Guarded Aggregate Queries
FastAPI backend for query execution and visualization
"""
from fastapi import FastAPI, WebSocket, HTTPException, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
import os
import logging

from app.api import queries, execution, hypergraph, data_import
from app.spark.spark_manager import SparkManager
from app.dependencies import set_spark_manager, get_spark_manager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def _prewarm():
    """Best-effort: auto-load a bundled dump and warm a few queries on startup.

    Opt-in via env so a container restart leaves the demo ready instead of an
    empty Spark session. Controlled by:
      SPARK_Y_AUTOLOAD_DUMP  e.g. "stats.sql.gz"   (filename in data/sql-dumps)
      SPARK_Y_AUTOLOAD_DB    e.g. "stats"          (target Postgres database)
      SPARK_Y_PREWARM_QUERIES e.g. "stats_009-033,stats_011-050" (optional)
    Runs in the background; any failure is logged and ignored.
    """
    # Fast path: load a previously materialised Parquet dataset. Registration is
    # lazy (no scan), so a restart comes back in seconds and needs no Postgres.
    #   SPARK_Y_AUTOLOAD_PARQUET  e.g. "imdb"  (dataset under data/parquet)
    parquet_db = os.getenv("SPARK_Y_AUTOLOAD_PARQUET")
    if parquet_db:
        try:
            from app.api.data_import import load_from_parquet
            await asyncio.sleep(5)
            logger.info(f"[prewarm] loading Parquet dataset '{parquet_db}'")
            await load_from_parquet(database=parquet_db)
            logger.info("[prewarm] Parquet dataset loaded")
            await _warm_queries()
        except Exception as e:
            logger.warning(f"[prewarm] Parquet autoload aborted: {e}")
        return

    dump = os.getenv("SPARK_Y_AUTOLOAD_DUMP")
    db = os.getenv("SPARK_Y_AUTOLOAD_DB")
    if not dump or not db:
        return
    try:
        from app.api.data_import import create_database, load_sql_dump_from_library
        # Give Postgres a moment to accept connections after compose start.
        await asyncio.sleep(5)
        logger.info(f"[prewarm] ensuring database '{db}' exists")
        try:
            await create_database(database_name=db, owner="postgres")
        except Exception as e:
            logger.info(f"[prewarm] create_database: {e} (likely already exists)")
        logger.info(f"[prewarm] loading dump '{dump}' into '{db}'")
        await load_sql_dump_from_library(dump, database=db)
        logger.info("[prewarm] dump loaded; warming queries")
        await _warm_queries()
        logger.info("[prewarm] done")
    except Exception as e:
        logger.warning(f"[prewarm] aborted: {e}")


async def _warm_queries():
    """Warm the JVM / plan cache with the SPARK_Y_PREWARM_QUERIES featured set."""
    ids = os.getenv("SPARK_Y_PREWARM_QUERIES", "")
    query_ids = [q.strip() for q in ids.split(",") if q.strip()]
    if not query_ids:
        return
    from app.api.queries import load_queries
    catalog = {q.query_id: q for q in load_queries()}
    sm = get_spark_manager()
    for qid in query_ids:
        q = catalog.get(qid)
        if not q:
            continue
        try:
            await sm.execute_query(sql=q.sql, use_optimization=True, collect_results=False)
            logger.info(f"[prewarm] warmed {qid}")
        except Exception as e:
            logger.info(f"[prewarm] warm {qid} failed: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize and cleanup Spark session"""
    logger.info("Initializing Spark session...")
    spark_manager = SparkManager()
    await spark_manager.initialize()
    set_spark_manager(spark_manager)
    logger.info("Spark session initialized successfully")

    # Kick off optional auto-load + warm-up without blocking startup.
    asyncio.create_task(_prewarm())

    yield

    logger.info("Shutting down Spark session...")
    await spark_manager.shutdown()
    logger.info("Spark session shut down successfully")


# Create FastAPI app
app = FastAPI(
    title="Query Optimization Demo",
    description="Interactive demonstration of avoiding materialization in aggregate queries",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS for Vue.js frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],  # Vite default port
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Include routers
app.include_router(queries.router, prefix="/api/queries", tags=["queries"])
app.include_router(execution.router, prefix="/api/execution", tags=["execution"])
app.include_router(hypergraph.router, prefix="/api/hypergraph", tags=["hypergraph"])
app.include_router(data_import.router, prefix="/api/data-import", tags=["data-import"])


@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "message": "Query Optimization Demo API",
        "status": "running",
        "docs": "/docs"
    }


@app.get("/api/health")
async def health_check():
    """Check if Spark is ready and get table/database info"""
    try:
        spark_manager = get_spark_manager()
        if spark_manager.is_ready():
            table_count = spark_manager.get_table_count()
            db_info = spark_manager.get_database_info()
            return {
                "status": "healthy",
                "spark": "ready",
                "table_count": table_count,
                "database": db_info
            }
    except HTTPException:
        pass
    return {"status": "unhealthy", "spark": "not ready", "table_count": 0, "database": None}
