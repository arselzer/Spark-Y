"""
API routes for query execution and comparison
"""
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, HTTPException
from typing import Dict, List
import asyncio
import logging
import re
import time
import uuid
from datetime import datetime
from pathlib import Path

from app.models.query import (
    ExecutionRequest, ExecutionResult, ExecutionProgress, SparkConfig,
    ExecutionMetrics, SavedRun, SavedRunSummary, SaveRunRequest,
    SavedBatch, SavedBatchSummary, SaveBatchRequest,
)
from app.spark.spark_manager import SparkManager
from app.dependencies import get_spark_manager

logger = logging.getLogger(__name__)
router = APIRouter()

# Track active executions
_active_executions: Dict[str, ExecutionProgress] = {}

# Saved-run library: one JSON file per run, same pattern as the SQL-dump and
# FK-map libraries in data_import.py. Lets the presenter pre-save runs and
# replay them instantly during the demo without re-executing.
SAVED_RUNS_DIR = Path("/app/data/saved-runs")
SAVED_RUNS_DIR.mkdir(parents=True, exist_ok=True)


def _saved_run_path(run_id: str) -> Path:
    """Resolve a run id to its file path, guarding against traversal."""
    path = SAVED_RUNS_DIR / f"{run_id}.json"
    if not path.resolve().is_relative_to(SAVED_RUNS_DIR.resolve()):
        raise HTTPException(status_code=403, detail="Access denied")
    return path

# Hard upper bound per execute call. The /execute route runs two queries in
# sequence (reference + optimised), so total worst-case latency is 2x this.
# 90s per phase keeps walk-up attendees from waiting more than ~3 minutes on
# a runaway Cartesian product before we cut them off.
EXECUTION_TIMEOUT_SECONDS = 90

# Block destructive / non-read statements before they reach Spark. The demo
# only needs to display analytical results — INSERTs, DROPs, etc. are never
# legitimate and have caused us pain when attendees paste random SQL.
_DESTRUCTIVE_STATEMENT = re.compile(
    r"^\s*(?:--[^\n]*\n|/\*.*?\*/|\s)*"  # skip leading comments / whitespace
    r"(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|GRANT|REVOKE|MERGE|REPLACE|CALL|USE|SET)\b",
    re.IGNORECASE | re.DOTALL,
)


def _assert_read_only(sql: str) -> None:
    """Reject queries that aren't pure reads. Raises HTTPException(400)."""
    if _DESTRUCTIVE_STATEMENT.match(sql or ""):
        raise HTTPException(
            status_code=400,
            detail="Only SELECT / WITH queries are allowed in the demo.",
        )


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

    _assert_read_only(request.sql)

    result = ExecutionResult(
        query_id=request.query_id,
        sql=request.sql,
        success=False
    )

    try:
        # Get configurations from request or use defaults.
        # Demo defaults from a config sweep against STATS-CEB:
        # - physicalCountJoinEnabled is required (yannakakis alone crashes
        #   with collectToPython errors on multi-way joins).
        # - countGroupInLeaves consistently regresses on the current Spark
        #   fork (2-3x shuffle blow-up). The paper's notebook turned it on
        #   for STATS, but the flag's semantics appear to have shifted.
        # - unguardedEnabled on so unguarded queries (some JOB queries)
        #   also exercise the optimisation; semantically a no-op for the
        #   guarded majority of STATS-CEB.
        reference_config = request.reference_config or SparkConfig(
            yannakakis_enabled=False,
            physical_count_join_enabled=False,
            unguarded_enabled=False,
            count_group_in_leaves=False,
        )

        optimized_config = request.optimized_config or SparkConfig(
            yannakakis_enabled=True,
            physical_count_join_enabled=True,
            unguarded_enabled=True,
            count_group_in_leaves=False,
        )

        # Execute reference query. If it times out, we *don't* abort the
        # whole request — many of the best demo queries are precisely the
        # ones where the unoptimised plan exceeds the budget. Record the
        # timeout, then still run the optimised side so the comparison
        # panel shows "ref: timed out" vs "opt: 1.2s".
        logger.info("Executing reference query with config: yannakakis=%s, physicalCountJoin=%s, unguarded=%s",
                   reference_config.yannakakis_enabled,
                   reference_config.physical_count_join_enabled,
                   reference_config.unguarded_enabled)
        reference_timed_out = False
        original_metrics = None
        original_results = None
        try:
            original_metrics, original_plan, original_results = await asyncio.wait_for(
                spark_manager.execute_query(
                    sql=request.sql,
                    use_optimization=False,
                    collect_results=True,
                    spark_config=reference_config,
                ),
                timeout=EXECUTION_TIMEOUT_SECONDS,
            )
            result.original_metrics = original_metrics
            result.original_plan = original_plan
        except asyncio.TimeoutError:
            reference_timed_out = True
            try:
                spark_manager.cancel_current_execution()
            except Exception as cancel_exc:
                logger.warning(f"Cancel after reference timeout failed: {cancel_exc}")
            logger.warning("Reference timed out after %ds; continuing with optimised run", EXECUTION_TIMEOUT_SECONDS)
            # Placeholder metrics flagged timed_out, with execution_time set to
            # the budget, so the comparison UI renders "Timeout (>Ns)" instead
            # of hiding the whole comparison. No plan/operator data exists.
            original_metrics = ExecutionMetrics(
                execution_time_ms=float(EXECUTION_TIMEOUT_SECONDS * 1000),
                timed_out=True,
            )
            result.original_metrics = original_metrics

        # Execute optimized query
        logger.info("Executing optimized query with config: yannakakis=%s, physicalCountJoin=%s, unguarded=%s",
                   optimized_config.yannakakis_enabled,
                   optimized_config.physical_count_join_enabled,
                   optimized_config.unguarded_enabled)
        optimized_metrics, optimized_plan, optimized_results = await asyncio.wait_for(
            spark_manager.execute_query(
                sql=request.sql,
                use_optimization=True,
                collect_results=True,
                spark_config=optimized_config,
            ),
            timeout=EXECUTION_TIMEOUT_SECONDS,
        )

        result.optimized_metrics = optimized_metrics
        result.optimized_plan = optimized_plan

        # Convert results to dicts (first few rows). Prefer reference rows
        # when present, otherwise fall back to the optimised run so the
        # results panel isn't empty when reference timed out.
        rows_for_preview = original_results or optimized_results
        if rows_for_preview:
            result.result_rows = [row.asDict() for row in rows_for_preview[:10]]
            result.result_schema = list(rows_for_preview[0].asDict().keys())

        # Calculate improvement metrics. Skip when the reference timed out:
        # its time is only a lower bound, so a precise speedup would mislead.
        if original_metrics and not original_metrics.timed_out and original_metrics.execution_time_ms > 0:
            result.speedup = original_metrics.execution_time_ms / max(
                optimized_metrics.execution_time_ms, 0.001
            )

        if original_metrics and original_metrics.intermediate_result_size_bytes > 0:
            result.memory_reduction = (
                1 - optimized_metrics.intermediate_result_size_bytes /
                original_metrics.intermediate_result_size_bytes
            )

        # Headline "avoided materialisation": the intermediate join rows the
        # reference plan processed that the optimised plan did not. Only
        # meaningful when the reference run completed (otherwise we don't know
        # its peak); the UI handles the timed-out case separately.
        if original_metrics and original_metrics.peak_intermediate_rows > 0:
            result.avoided_intermediate_rows = max(
                0,
                original_metrics.peak_intermediate_rows
                - optimized_metrics.peak_intermediate_rows,
            )

        if reference_timed_out:
            # Surface the timeout in error so the UI can render a clear note,
            # but keep success=True because the optimised side returned data.
            result.success = True
            result.error = f"Reference exceeded the {EXECUTION_TIMEOUT_SECONDS}s budget and was cancelled; optimised run completed."
            logger.info("Reference timed out; optimised completed in %.0fms", optimized_metrics.execution_time_ms)
        else:
            result.success = True
            logger.info(
                "Query executed successfully. Speedup: %s",
                f"{result.speedup:.2f}x" if result.speedup else "n/a",
            )

    except asyncio.TimeoutError:
        # Optimised side timed out too — this is the kiosk-runaway case.
        try:
            spark_manager.cancel_current_execution()
        except Exception as cancel_exc:
            logger.warning(f"Cancel after timeout failed: {cancel_exc}")
        logger.warning("Query execution timed out after %ds", EXECUTION_TIMEOUT_SECONDS)
        result.success = False
        result.error = f"Query exceeded the {EXECUTION_TIMEOUT_SECONDS}s execution budget and was cancelled."
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Query execution failed: {e}", exc_info=True)
        result.success = False
        result.error = str(e)

    return result


@router.post("/cancel")
async def cancel_execution(spark_manager: SparkManager = Depends(get_spark_manager)):
    """Cancel the currently-running query, if any.

    Wired to the frontend cancel button so users can stop a long query without
    refreshing the page or blocking the next attendee.
    """
    cancelled = spark_manager.cancel_current_execution()
    if cancelled:
        return {"cancelled": True, "job_group": cancelled}
    return {"cancelled": False, "message": "No active query to cancel."}


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


# ============================================================================
# Saved Runs — persist execution results for instant replay during the demo
# ============================================================================

def _summarise(run: SavedRun) -> SavedRunSummary:
    """Project a full saved run down to list-view metadata."""
    r = run.result
    return SavedRunSummary(
        id=run.id,
        name=run.name,
        query_id=run.query_id,
        saved_at=run.saved_at,
        sql=r.sql,
        speedup=r.speedup,
        avoided_intermediate_rows=r.avoided_intermediate_rows,
        reference_timed_out=bool(r.original_metrics and r.original_metrics.timed_out),
    )


@router.post("/runs", response_model=SavedRunSummary)
async def save_run(request: SaveRunRequest):
    """Persist an execution result so it can be replayed without re-running."""
    result = request.result
    run_id = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

    # Default name: query id, else a short SQL snippet.
    default_name = result.query_id or " ".join(result.sql.split())[:60]
    run = SavedRun(
        id=run_id,
        name=(request.name or default_name).strip() or run_id,
        query_id=result.query_id,
        saved_at=datetime.now().isoformat(timespec="seconds"),
        result=result,
    )

    try:
        _saved_run_path(run_id).write_text(run.model_dump_json(indent=2))
    except Exception as e:
        logger.error(f"Failed to save run: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to save run: {e}")

    logger.info(f"Saved run {run_id} ({run.name})")
    return _summarise(run)


@router.get("/runs", response_model=List[SavedRunSummary])
async def list_saved_runs():
    """List saved runs, newest first."""
    summaries: List[SavedRunSummary] = []
    for path in sorted(SAVED_RUNS_DIR.glob("*.json"), reverse=True):
        try:
            run = SavedRun.model_validate_json(path.read_text())
            summaries.append(_summarise(run))
        except Exception as e:
            logger.warning(f"Skipping unreadable saved run {path.name}: {e}")
    return summaries


@router.get("/runs/{run_id}", response_model=SavedRun)
async def get_saved_run(run_id: str):
    """Load a full saved run for instant replay."""
    path = _saved_run_path(run_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Saved run not found")
    try:
        return SavedRun.model_validate_json(path.read_text())
    except Exception as e:
        logger.error(f"Failed to read saved run {run_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to read saved run: {e}")


@router.delete("/runs/{run_id}")
async def delete_saved_run(run_id: str):
    """Delete a saved run from the library."""
    path = _saved_run_path(run_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Saved run not found")
    path.unlink()
    logger.info(f"Deleted saved run {run_id}")
    return {"success": True, "id": run_id}


# ── Saved batches ───────────────────────────────────────────────────────────
# A batch run (a set of query results from the Batch page) persisted as one
# unit, so a whole sweep can be reopened and inspected later without re-running.
SAVED_BATCHES_DIR = Path("/app/data/saved-batches")
SAVED_BATCHES_DIR.mkdir(parents=True, exist_ok=True)


def _saved_batch_path(batch_id: str) -> Path:
    """Resolve a batch id to its file path, guarding against traversal."""
    path = SAVED_BATCHES_DIR / f"{batch_id}.json"
    if not path.resolve().is_relative_to(SAVED_BATCHES_DIR.resolve()):
        raise HTTPException(status_code=403, detail="Access denied")
    return path


def _summarise_batch(batch: SavedBatch) -> SavedBatchSummary:
    """Project a full saved batch down to list-view aggregate metadata."""
    total_ref_ms = 0.0
    total_opt_ms = 0.0
    total_avoided = 0
    timeouts = 0
    for item in batch.items:
        r = item.result
        om = r.original_metrics
        pm = r.optimized_metrics
        if om and pm:
            total_ref_ms += om.execution_time_ms or 0
            total_opt_ms += pm.execution_time_ms or 0
        if r.avoided_intermediate_rows:
            total_avoided += r.avoided_intermediate_rows
        if om and om.timed_out:
            timeouts += 1
    overall = (total_ref_ms / total_opt_ms) if total_opt_ms > 0 else None
    return SavedBatchSummary(
        id=batch.id,
        name=batch.name,
        saved_at=batch.saved_at,
        query_count=len(batch.items),
        overall_speedup=overall,
        total_avoided_intermediate_rows=total_avoided,
        timeout_count=timeouts,
    )


@router.post("/batches", response_model=SavedBatchSummary)
async def save_batch(request: SaveBatchRequest):
    """Persist a batch run so the whole sweep can be reopened later."""
    if not request.items:
        raise HTTPException(status_code=400, detail="Cannot save an empty batch")

    batch_id = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    default_name = f"Batch · {len(request.items)} queries · {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    batch = SavedBatch(
        id=batch_id,
        name=(request.name or default_name).strip() or batch_id,
        saved_at=datetime.now().isoformat(timespec="seconds"),
        items=request.items,
    )
    try:
        _saved_batch_path(batch_id).write_text(batch.model_dump_json(indent=2))
    except Exception as e:
        logger.error(f"Failed to save batch: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to save batch: {e}")

    logger.info(f"Saved batch {batch_id} ({batch.name}, {len(batch.items)} queries)")
    return _summarise_batch(batch)


@router.get("/batches", response_model=List[SavedBatchSummary])
async def list_saved_batches():
    """List saved batches, newest first."""
    summaries: List[SavedBatchSummary] = []
    for path in sorted(SAVED_BATCHES_DIR.glob("*.json"), reverse=True):
        try:
            batch = SavedBatch.model_validate_json(path.read_text())
            summaries.append(_summarise_batch(batch))
        except Exception as e:
            logger.warning(f"Skipping unreadable saved batch {path.name}: {e}")
    return summaries


@router.get("/batches/{batch_id}", response_model=SavedBatch)
async def get_saved_batch(batch_id: str):
    """Load a full saved batch for viewing."""
    path = _saved_batch_path(batch_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Saved batch not found")
    try:
        return SavedBatch.model_validate_json(path.read_text())
    except Exception as e:
        logger.error(f"Failed to read saved batch {batch_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to read saved batch: {e}")


@router.delete("/batches/{batch_id}")
async def delete_saved_batch(batch_id: str):
    """Delete a saved batch from the library."""
    path = _saved_batch_path(batch_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Saved batch not found")
    path.unlink()
    logger.info(f"Deleted saved batch {batch_id}")
    return {"success": True, "id": batch_id}
