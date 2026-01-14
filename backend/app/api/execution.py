"""
API routes for query execution and comparison
"""
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, HTTPException
from typing import Dict
import asyncio
import logging
import time

from app.models.query import ExecutionRequest, ExecutionResult, ExecutionProgress, SparkConfig
from app.spark.spark_manager import SparkManager
from app.dependencies import get_spark_manager

logger = logging.getLogger(__name__)
router = APIRouter()

# Track active executions
_active_executions: Dict[str, ExecutionProgress] = {}


@router.post("/execute", response_model=ExecutionResult)
async def execute_query(
    request: ExecutionRequest,
    spark_manager: SparkManager = Depends(get_spark_manager)
):
    """
    Execute a query and compare original vs optimized execution

    - **query_id**: Optional query ID from catalog
    - **sql**: SQL query to execute
    - **use_optimization**: Enable optimization (runs both for comparison)
    - **collect_metrics**: Collect detailed execution metrics
    """
    logger.info(f"Executing query: {request.query_id or 'custom'}")

    result = ExecutionResult(
        query_id=request.query_id,
        sql=request.sql,
        success=False
    )

    try:
        # Get configurations from request or use defaults
        reference_config = request.reference_config or SparkConfig(
            yannakakis_enabled=False,
            physical_count_join_enabled=False,
            unguarded_enabled=False
        )

        optimized_config = request.optimized_config or SparkConfig(
            yannakakis_enabled=True,
            physical_count_join_enabled=True,
            unguarded_enabled=True
        )

        # Execute reference query
        logger.info("Executing reference query with config: yannakakis=%s, physicalCountJoin=%s, unguarded=%s",
                   reference_config.yannakakis_enabled,
                   reference_config.physical_count_join_enabled,
                   reference_config.unguarded_enabled)
        original_metrics, original_plan, original_results = await spark_manager.execute_query(
            sql=request.sql,
            use_optimization=False,
            collect_results=True,
            spark_config=reference_config
        )

        result.original_metrics = original_metrics
        result.original_plan = original_plan

        # Execute optimized query
        logger.info("Executing optimized query with config: yannakakis=%s, physicalCountJoin=%s, unguarded=%s",
                   optimized_config.yannakakis_enabled,
                   optimized_config.physical_count_join_enabled,
                   optimized_config.unguarded_enabled)
        optimized_metrics, optimized_plan, optimized_results = await spark_manager.execute_query(
            sql=request.sql,
            use_optimization=True,
            collect_results=True,
            spark_config=optimized_config
        )

        result.optimized_metrics = optimized_metrics
        result.optimized_plan = optimized_plan

        # Convert results to dicts (first few rows)
        if original_results:
            result.result_rows = [row.asDict() for row in original_results[:10]]
            result.result_schema = list(original_results[0].asDict().keys())

        # Calculate improvement metrics
        if original_metrics.execution_time_ms > 0:
            result.speedup = original_metrics.execution_time_ms / max(
                optimized_metrics.execution_time_ms, 0.001
            )

        if original_metrics.intermediate_result_size_bytes > 0:
            result.memory_reduction = (
                1 - optimized_metrics.intermediate_result_size_bytes /
                original_metrics.intermediate_result_size_bytes
            )

        result.success = True
        logger.info(f"Query executed successfully. Speedup: {result.speedup:.2f}x")

    except Exception as e:
        logger.error(f"Query execution failed: {e}", exc_info=True)
        result.success = False
        result.error = str(e)

    return result


@router.websocket("/stream/{execution_id}")
async def execution_stream(
    websocket: WebSocket,
    execution_id: str
):
    """
    WebSocket endpoint for streaming execution progress

    Provides real-time updates during query execution including:
    - Current stage
    - Progress percentage
    - Intermediate result sizes
    """
    await websocket.accept()

    logger.info(f"WebSocket connected for execution: {execution_id}")

    try:
        # Simulated progress updates
        # In a real implementation, this would connect to Spark's listener API
        stages = [
            "Parsing query",
            "Analyzing logical plan",
            "Optimizing plan",
            "Generating physical plan",
            "Executing stage 1/3",
            "Executing stage 2/3",
            "Executing stage 3/3",
            "Collecting results"
        ]

        for i, stage in enumerate(stages):
            progress = ExecutionProgress(
                stage=stage,
                progress_percent=(i + 1) / len(stages) * 100,
                current_task=stage,
                intermediate_size_bytes=1024 * (i + 1) * 100
            )

            await websocket.send_json(progress.dict())
            await asyncio.sleep(0.5)  # Simulate work

        await websocket.send_json({
            "stage": "Complete",
            "progress_percent": 100,
            "current_task": "Done"
        })

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected: {execution_id}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}", exc_info=True)
        await websocket.close()


@router.post("/explain")
async def explain_query(
    request: ExecutionRequest,
    spark_manager: SparkManager = Depends(get_spark_manager)
):
    """
    Get execution plan without running the query

    Returns both logical and physical plans for analysis
    """
    try:
        # This is a lightweight operation - just get the plan
        spark = spark_manager.spark
        df = spark.sql(request.sql)

        logical_plan = df._jdf.queryExecution().logical().toString()
        optimized_logical = df._jdf.queryExecution().optimizedPlan().toString()
        physical_plan = df._jdf.queryExecution().executedPlan().toString()

        return {
            "logical_plan": logical_plan,
            "optimized_logical_plan": optimized_logical,
            "physical_plan": physical_plan
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Query analysis failed: {str(e)}")


@router.get("/history")
async def get_execution_history(limit: int = 20):
    """
    Get recent query execution history

    In a full implementation, this would be backed by a database
    """
    # Placeholder - would load from database
    return {
        "executions": [],
        "message": "History tracking not yet implemented"
    }
