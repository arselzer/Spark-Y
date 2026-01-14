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
    peak_memory_mb: Optional[float] = None
    num_stages: int = 0

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


class ExecutionProgress(BaseModel):
    """Progress update during execution"""
    stage: str
    progress_percent: float
    current_task: str
    intermediate_size_bytes: Optional[int] = None
