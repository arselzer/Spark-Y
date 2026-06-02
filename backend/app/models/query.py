"""
Pydantic models for query execution and results
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from enum import Enum


class QueryCategory(str, Enum):
    """Query benchmark categories"""
    JOB = "job"
    TPCH = "tpch"
    TPCDS = "tpcds"
    STATS_CEB = "stats-ceb"
    SNAP = "snap"
    CUSTOM = "custom"


class QueryMetadata(BaseModel):
    """Metadata for a SQL query"""
    query_id: str
    name: str
    category: QueryCategory
    description: Optional[str] = None
    sql: str
    tables: List[str] = []
    num_joins: int = 0
    num_aggregates: int = 0


class SparkConfig(BaseModel):
    """Spark SQL configuration options"""
    yannakakis_enabled: bool = False
    physical_count_join_enabled: bool = False
    unguarded_enabled: bool = False
    # countGroupInLeaves: push partial count aggregation down into the leaf
    # relations before the CountJoin chain. Required to see STATS-CEB-style
    # speedups (see arselzer/spark-eval benchmark_all(..., group_in_leaves=True)).
    count_group_in_leaves: bool = False
    # Allow custom config options
    custom_options: Optional[Dict[str, str]] = None


class ExecutionRequest(BaseModel):
    """Request to execute a query"""
    query_id: Optional[str] = None
    sql: str
    use_optimization: bool = True
    collect_metrics: bool = True
    # Configuration for reference and optimized executions
    reference_config: Optional[SparkConfig] = None
    optimized_config: Optional[SparkConfig] = None


class ExecutionMetrics(BaseModel):
    """Metrics from query execution"""
    execution_time_ms: float
    planning_time_ms: Optional[float] = None
    total_input_rows: int = 0
    total_output_rows: int = 0
    intermediate_result_size_bytes: int = 0
    avoided_materialization_bytes: int = 0
    # Largest number of rows emitted by any join operator in this plan, i.e.
    # the peak intermediate join result the plan had to process. This is the
    # honest "materialisation" measure: a guarded aggregate query may emit
    # billions of join tuples in the reference plan to return a single row.
    peak_intermediate_rows: int = 0
    peak_memory_mb: Optional[float] = None
    num_stages: int = 0
    # True when this side did not finish within the execution budget. The
    # metrics are then a placeholder (execution_time_ms = the budget) so the
    # comparison UI can render "Timeout (>Ns)" instead of a real figure.
    timed_out: bool = False

    # Detailed metrics for enhanced comparison
    operator_counts: Optional[Dict[str, int]] = None
    shuffle_read_bytes: int = 0
    shuffle_write_bytes: int = 0
    num_tasks: int = 0
    stages: Optional[List[Dict[str, Any]]] = None  # Stage-level information for timeline visualization
    operator_metrics: Optional[List[Dict[str, Any]]] = None  # Operator-level metrics from execution plan


class ExecutionPlan(BaseModel):
    """Physical execution plan"""
    plan_string: str
    plan_tree: Optional[Dict[str, Any]] = None  # Structured JSON representation of physical plan
    analyzed_logical_plan_json: Optional[str] = None   # JSON string of analyzed logical plan
    optimized_logical_plan_json: Optional[str] = None  # JSON string of optimized logical plan
    logical_plan_json: Optional[str] = None      # JSON string of logical plan (deprecated, use optimized_logical_plan_json)
    physical_plan_json: Optional[str] = None     # JSON string of physical plan
    optimizations_applied: List[str] = []


class ExecutionResult(BaseModel):
    """Result of query execution"""
    query_id: Optional[str] = None
    sql: str
    success: bool
    error: Optional[str] = None

    # Execution details
    original_metrics: Optional[ExecutionMetrics] = None
    optimized_metrics: Optional[ExecutionMetrics] = None

    original_plan: Optional[ExecutionPlan] = None
    optimized_plan: Optional[ExecutionPlan] = None

    # Results preview
    result_rows: Optional[List[Dict[str, Any]]] = None
    result_schema: Optional[List[str]] = None

    # Improvement statistics
    speedup: Optional[float] = None
    memory_reduction: Optional[float] = None
    # Intermediate join rows the optimised plan avoided materialising, i.e.
    # reference.peak_intermediate_rows - optimised.peak_intermediate_rows.
    # The headline "avoided materialisation" number for the demo.
    avoided_intermediate_rows: Optional[int] = None


class ExecutionProgress(BaseModel):
    """Progress update during execution"""
    stage: str
    progress_percent: float
    current_task: str
    intermediate_size_bytes: Optional[int] = None


class SavedRun(BaseModel):
    """A persisted execution result, replayable without re-running the query."""
    id: str
    name: str
    query_id: Optional[str] = None
    saved_at: str            # ISO timestamp
    result: ExecutionResult  # full result for instant replay


class SavedRunSummary(BaseModel):
    """Lightweight saved-run metadata for list views (no plans/operators)."""
    id: str
    name: str
    query_id: Optional[str] = None
    saved_at: str
    sql: str
    speedup: Optional[float] = None
    avoided_intermediate_rows: Optional[int] = None
    reference_timed_out: bool = False


class SaveRunRequest(BaseModel):
    """Request to persist an execution result."""
    name: Optional[str] = None
    result: ExecutionResult


class SavedBatchItem(BaseModel):
    """One query's result within a saved batch."""
    query_id: Optional[str] = None
    name: str
    result: ExecutionResult


class SavedBatch(BaseModel):
    """A persisted batch run: a set of query results viewable later."""
    id: str
    name: str
    saved_at: str            # ISO timestamp
    items: List[SavedBatchItem]


class SavedBatchSummary(BaseModel):
    """Lightweight saved-batch metadata for list views (no per-query plans)."""
    id: str
    name: str
    saved_at: str
    query_count: int
    overall_speedup: Optional[float] = None
    total_avoided_intermediate_rows: int = 0
    timeout_count: int = 0


class SaveBatchRequest(BaseModel):
    """Request to persist a batch run."""
    name: Optional[str] = None
    items: List[SavedBatchItem]
