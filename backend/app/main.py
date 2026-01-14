"""
Query Optimization Demo: Avoiding Materialization for Guarded Aggregate Queries
FastAPI backend for query execution and visualization
"""
from fastapi import FastAPI, WebSocket, HTTPException, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging

from app.api import queries, execution, hypergraph, data_import
from app.spark.spark_manager import SparkManager
from app.dependencies import set_spark_manager, get_spark_manager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize and cleanup Spark session"""
    logger.info("Initializing Spark session...")
    spark_manager = SparkManager()
    await spark_manager.initialize()
    set_spark_manager(spark_manager)
    logger.info("Spark session initialized successfully")

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
