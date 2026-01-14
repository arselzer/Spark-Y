"""
Spark session manager and query executor
Integrates with custom Spark build containing optimization catalyst rules
"""
import asyncio
import time
import logging
import re
import json
from typing import Optional, Tuple, Any, List, Dict
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf

from app.models.query import ExecutionMetrics, ExecutionPlan, ExecutionResult, SparkConfig
from app.spark.metrics_collector import MetricsCollector
from app.spark.stage_metrics_factory import create_stage_metrics_collector, get_collector_info
from app.spark.operator_metrics_collector import OperatorMetricsCollector

logger = logging.getLogger(__name__)


class SparkManager:
    """Manages Spark session and query execution"""

    def __init__(self):
        self.spark: Optional[SparkSession] = None
        self._initialized = False
        self.metrics_collector = MetricsCollector()
        self.stage_metrics_collector = None  # Created during initialization
        self.operator_metrics_collector = None  # Created during initialization
        self.current_database: Optional[str] = None  # Track current connected database

    async def initialize(self):
        """Initialize Spark session with custom configuration"""
        if self._initialized:
            return

        # Run initialization in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._init_spark)
        self._initialized = True

    def _init_spark(self):
        """Actual Spark initialization (blocking)"""
        logger.info("Initializing Spark session...")

        # Get memory settings from environment variables with sensible defaults
        import os
        driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "16g")
        executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY", "16g")
        pyspark_variant = os.getenv("PYSPARK_VARIANT", "standard")

        logger.info(f"PySpark variant: {pyspark_variant}")
        logger.info(f"Configuring Spark with driver memory: {driver_memory}, executor memory: {executor_memory}")

        # Log PySpark version and location
        try:
            import pyspark
            logger.info(f"PySpark version: {pyspark.__version__}")
            logger.info(f"PySpark location: {pyspark.__file__}")
        except Exception as e:
            logger.warning(f"Could not get PySpark version: {e}")

        # Configuration for custom Spark build
        conf = SparkConf()
        conf.set("spark.app.name", "Yannasparkis-Query-Optimization")
        conf.set("spark.master", "local[*]")
        conf.set("spark.driver.memory", driver_memory)
        conf.set("spark.executor.memory", executor_memory)

        # Memory management settings for better handling of large data imports
        conf.set("spark.memory.fraction", "0.8")  # Use 80% of heap for execution and storage
        conf.set("spark.memory.storageFraction", "0.3")  # 30% of memory for caching, 70% for execution
        conf.set("spark.driver.maxResultSize", "4g")  # Max size of results that can be sent to driver

        conf.set("spark.sql.adaptive.enabled", "false")  # Disable AQE for consistent comparison
        conf.set("spark.sql.cbo.enabled", "true")  # Enable cost-based optimization

        # Add custom Spark JAR and JDBC drivers if available
        jars = []

        custom_jar_path = Path("/app/spark/custom-spark-sql.jar")
        if custom_jar_path.exists() and custom_jar_path.is_file():
            jars.append(str(custom_jar_path))
            logger.info(f"Using custom Spark JAR: {custom_jar_path}")
        elif custom_jar_path.exists():
            logger.warning(f"Custom Spark JAR path exists but is not a file: {custom_jar_path}")
        else:
            logger.info("Custom Spark JAR not found, using standard PySpark")

        # Add PostgreSQL JDBC driver
        postgres_jar_path = Path("/app/jars/postgresql-42.7.1.jar")
        if postgres_jar_path.exists() and postgres_jar_path.is_file():
            jars.append(str(postgres_jar_path))
            logger.info(f"Using PostgreSQL JDBC driver: {postgres_jar_path}")
        else:
            logger.warning(f"PostgreSQL JDBC driver not found at {postgres_jar_path}")

        if jars:
            conf.set("spark.jars", ",".join(jars))

        # Create Spark session
        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()

        # Set log level
        self.spark.sparkContext.setLogLevel("WARN")

        # Initialize stage metrics collector (uses factory to choose implementation)
        # Defaults to REST API for stability and cross-version compatibility
        self.stage_metrics_collector = create_stage_metrics_collector(self.spark)
        collector_info = get_collector_info(self.stage_metrics_collector)
        logger.info(f"Stage metrics collector initialized: {collector_info['type']} ({collector_info['description']})")

        # Initialize operator metrics collector
        self.operator_metrics_collector = OperatorMetricsCollector(self.spark)
        logger.info("Operator metrics collector initialized")

        logger.info("Spark session created successfully")

    async def shutdown(self):
        """Shutdown Spark session"""
        if self.spark:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.spark.stop)
            self._initialized = False
            logger.info("Spark session stopped")

    def is_ready(self) -> bool:
        """Check if Spark is ready"""
        return self._initialized and self.spark is not None

    async def execute_query(
        self,
        sql: str,
        use_optimization: bool = True,
        collect_results: bool = True,
        spark_config: Optional[SparkConfig] = None
    ) -> Tuple[ExecutionMetrics, ExecutionPlan, Optional[Any]]:
        """
        Execute a SQL query and collect metrics

        Args:
            sql: SQL query string
            use_optimization: Enable custom optimization rules (deprecated, use spark_config)
            collect_results: Whether to collect results (may be expensive)
            spark_config: Spark SQL configuration options to apply

        Returns:
            Tuple of (metrics, plan, results)
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self._execute_query_sync,
            sql,
            use_optimization,
            collect_results,
            spark_config
        )

    def _execute_query_sync(
        self,
        sql: str,
        use_optimization: bool,
        collect_results: bool,
        spark_config: Optional[SparkConfig] = None
    ) -> Tuple[ExecutionMetrics, ExecutionPlan, Optional[Any]]:
        """Synchronous query execution"""
        if not self.spark:
            raise RuntimeError("Spark not initialized")

        # Clear Spark's cached query plans to ensure fresh plan generation with new config
        # This is critical because Spark caches plans by SQL text, not by configuration
        try:
            self.spark.catalog.clearCache()
            logger.debug("Cleared Spark plan cache before execution")
        except Exception as e:
            logger.debug(f"Could not clear Spark cache: {e}")

        # Apply Spark configuration if provided
        if spark_config:
            self._apply_spark_config(spark_config)
        else:
            # Legacy behavior: toggle single optimization flag
            if use_optimization:
                self.spark.conf.set("spark.sql.optimizer.avoidMaterialization.enabled", "true")
            else:
                self.spark.conf.set("spark.sql.optimizer.avoidMaterialization.enabled", "false")

        # Measure planning time
        planning_start = time.time()
        df = self.spark.sql(sql)
        query_execution = df._jdf.queryExecution()

        # Extract plan strings
        logical_plan = query_execution.logical().toString()
        physical_plan = query_execution.executedPlan().toString()

        # Log plan hash to verify different plans are generated
        import hashlib
        plan_hash = hashlib.md5(physical_plan.encode()).hexdigest()[:8]
        logger.info(f"Physical plan hash: {plan_hash}")

        # Try to extract JSON representation of plans
        analyzed_logical_plan_json = None
        optimized_logical_plan_json = None
        logical_plan_json = None  # Deprecated field, kept for backwards compatibility
        physical_plan_json = None

        try:
            # Extract analyzed logical plan (after binding to catalog/schema)
            analyzed_logical_plan_json = query_execution.analyzed().toJSON()
            logger.debug("Successfully extracted analyzed logical plan as JSON")
        except Exception as e:
            logger.debug(f"Could not extract analyzed logical plan JSON: {e}")

        try:
            # Extract optimized logical plan (after Catalyst optimizations)
            optimized_logical_plan_json = query_execution.optimizedPlan().toJSON()
            logger.debug("Successfully extracted optimized logical plan as JSON")
            # Keep old field for backwards compatibility
            logical_plan_json = optimized_logical_plan_json
        except Exception as e:
            logger.debug(f"Could not extract optimized logical plan JSON: {e}")

        try:
            # Build plan tree directly from operator structure instead of JSON
            # This preserves the hierarchical parent-child relationships
            # NOTE: Metrics will be empty at this point - we rebuild after execution
            executed_plan = query_execution.executedPlan()
            plan_tree_before_execution = self._build_plan_tree_from_operator(executed_plan)
            logger.debug("Successfully built plan tree from operator structure (before execution)")
            # Also keep JSON for backwards compatibility
            physical_plan_json = executed_plan.toJSON()
        except Exception as e:
            logger.debug(f"Could not extract physical plan: {e}")
            physical_plan_json = None
            plan_tree_before_execution = None

        planning_time = (time.time() - planning_start) * 1000

        # Track SQL execution ID for operator-level metrics
        # Get the last SQL execution ID before running the query
        sql_execution_id = None
        try:
            # Access Spark's SQL execution tracking
            # The execution ID will be available after df.collect() or df.count()
            # We'll capture it after execution
            pass
        except Exception as e:
            logger.debug(f"Could not access SQL execution tracking: {e}")

        # Track job IDs that are created during this execution
        # Get the current maximum job ID before execution
        status_tracker = self.spark.sparkContext.statusTracker()
        try:
            # Get the highest job ID seen so far by checking a wide range
            # We need to check recent jobs, not just active ones (which may be empty if jobs complete quickly)
            job_id_before = -1

            # Check the last 100 job IDs to find the current maximum
            # This is much more reliable than just checking active jobs
            for i in range(0, 100):
                try:
                    if status_tracker.getJobInfo(i):
                        job_id_before = max(job_id_before, i)
                except Exception:
                    pass

            logger.debug(f"Job ID before execution: {job_id_before}")
        except Exception as e:
            logger.debug(f"Could not determine job ID before execution: {e}")
            job_id_before = -1

        # Set a job group to identify jobs from this query
        import uuid
        job_group_id = f"query_{uuid.uuid4().hex[:8]}"
        self.spark.sparkContext.setJobGroup(job_group_id, sql[:100])  # First 100 chars of SQL as description
        logger.debug(f"Set job group ID: {job_group_id}")

        # Measure execution time
        execution_start = time.time()

        if collect_results:
            results = df.collect()
            result_count = len(results)
        else:
            result_count = df.count()
            results = None

        execution_time = (time.time() - execution_start) * 1000

        # Try to capture SQL execution ID from the query execution
        # This ID is used by Spark's SQL tab and is different from job IDs
        try:
            # Access the execution ID through Spark's SQL listener
            # The execution ID is assigned when the query is executed
            jvm = self.spark.sparkContext._jvm
            if hasattr(jvm.org.apache.spark, 'sql'):
                # Try to get the execution ID from the listener
                # This is internal API and may not always be available
                execution_listener = self.spark._jsparkSession.sessionState().listenerManager()
                # The execution ID should be the most recent one
                # We'll use the operator_metrics_collector to query for it via REST API
                # which is more reliable than accessing internal APIs
                pass
        except Exception as e:
            logger.debug(f"Could not access SQL execution ID via internal API: {e}")

        # Clear the job group immediately after execution
        self.spark.sparkContext.setJobGroup(None, None)

        # Get job IDs created during this execution using job group ID
        # This is more reliable than tracking before/after job IDs
        all_job_ids = []
        try:
            # Try to get jobs by job group via REST API if available
            if hasattr(self, 'stage_metrics_collector') and self.stage_metrics_collector:
                all_job_ids = self.stage_metrics_collector.get_jobs_by_group(job_group_id)
                if all_job_ids:
                    logger.info(f"Found {len(all_job_ids)} jobs via job group '{job_group_id}': {all_job_ids}")

            # Fallback: Use job ID range tracking if REST API method doesn't work
            if not all_job_ids:
                logger.debug("Falling back to job ID range tracking")
                # Find the current maximum job ID by checking a wide range after execution
                job_id_after = job_id_before

                # Check up to 100 jobs after job_id_before to find the new maximum
                for i in range(max(0, job_id_before), job_id_before + 100):
                    try:
                        job_info = status_tracker.getJobInfo(i)
                        if job_info:
                            job_id_after = max(job_id_after, i)
                    except Exception:
                        pass

                # Collect all jobs created during execution (job ID > job_id_before)
                for job_id in range(job_id_before + 1, job_id_after + 1):
                    try:
                        job_info = status_tracker.getJobInfo(job_id)
                        if job_info and job_info.status in ['SUCCEEDED', 'RUNNING', 'FAILED']:
                            all_job_ids.append(job_id)
                            logger.debug(f"Found job {job_id} with status {job_info.status}")
                    except Exception:
                        pass

                logger.info(f"Job ID tracking (fallback): before={job_id_before}, after={job_id_after}, collected job IDs: {all_job_ids}")
        except Exception as e:
            logger.warning(f"Could not collect job IDs: {e}")
            all_job_ids = []

        # Small delay to ensure metrics are written to UI
        import time as time_module
        time_module.sleep(0.2)  # Increased from 0.1 to 0.2 for SQL execution registration

        # Rebuild plan tree AFTER execution to get populated metrics
        # Metrics are only available after the query has been executed
        plan_tree = None
        try:
            executed_plan_after = query_execution.executedPlan()
            plan_tree = self._build_plan_tree_from_operator(executed_plan_after)
            logger.debug("Successfully rebuilt plan tree with populated metrics (after execution)")
        except Exception as e:
            logger.warning(f"Could not rebuild plan tree after execution: {e}")
            # Fall back to the tree built before execution (without metrics)
            plan_tree = plan_tree_before_execution

        # Extract metrics from Spark UI / listener
        # Pass the plan_tree for operator metrics extraction
        metrics = self._extract_metrics(df, execution_time, planning_time, all_job_ids, plan_tree)

        # Extract execution plan
        plan = ExecutionPlan(
            plan_string=physical_plan,
            plan_tree=plan_tree,
            analyzed_logical_plan_json=analyzed_logical_plan_json,
            optimized_logical_plan_json=optimized_logical_plan_json,
            logical_plan_json=logical_plan_json,  # Deprecated, kept for backwards compatibility
            physical_plan_json=physical_plan_json,
            optimizations_applied=self._detect_optimizations(physical_plan)
        )

        return metrics, plan, results

    def _extract_metrics(
        self,
        df: DataFrame,
        execution_time: float,
        planning_time: float,
        job_ids: List[int],
        plan_tree: Optional[Dict[str, Any]] = None
    ) -> ExecutionMetrics:
        """Extract comprehensive execution metrics from DataFrame"""

        # Reset metrics collector for new query
        self.metrics_collector.reset()

        try:
            # Access query execution
            query_execution = df._jdf.queryExecution()
            physical_plan = query_execution.executedPlan().toString()

            # Collect detailed metrics from plan
            detailed_metrics = self.metrics_collector.collect_from_query_execution(query_execution)

            # Extract operator counts
            operator_counts = detailed_metrics.get('operator_counts', {})

            # Get stage count from plan
            num_stages_from_plan = detailed_metrics.get('stage_count', 0)

            # Collect real stage-level metrics if available
            real_stage_metrics = []
            aggregated_metrics = {}

            if self.stage_metrics_collector and job_ids:
                try:
                    logger.debug(f"Collecting real stage metrics for {len(job_ids)} jobs: {job_ids}")
                    real_stage_metrics = self.stage_metrics_collector.collect_stage_metrics(job_ids)
                    logger.info(f"Collected real metrics for {len(real_stage_metrics)} stages")

                    # Aggregate real metrics
                    if real_stage_metrics:
                        # Log individual stage shuffle metrics BEFORE aggregation
                        for i, stage in enumerate(real_stage_metrics):
                            logger.info(f"Stage {i} (ID={stage.get('stage_id', 'N/A')}): "
                                       f"shuffle_read={stage.get('shuffle_read_bytes', 0)} bytes, "
                                       f"shuffle_write={stage.get('shuffle_write_bytes', 0)} bytes")

                        aggregated_metrics = self.stage_metrics_collector.aggregate_metrics_by_stage(real_stage_metrics)
                        logger.info(f"Aggregated row counts - Input: {aggregated_metrics.get('total_input_records', 0)}, Output: {aggregated_metrics.get('total_output_records', 0)}")
                        logger.info(f"Aggregated shuffle metrics - Read: {aggregated_metrics.get('total_shuffle_read_bytes', 0)} bytes, Write: {aggregated_metrics.get('total_shuffle_write_bytes', 0)} bytes")
                except Exception as e:
                    logger.warning(f"Could not collect real stage metrics: {e}", exc_info=True)
            else:
                if not self.stage_metrics_collector:
                    logger.warning("Stage metrics collector is not initialized")
                if not job_ids:
                    logger.warning("No job IDs available for metrics collection")

            # Use real metrics if available, otherwise show warning
            if real_stage_metrics:
                # Use real stage metrics
                num_stages = aggregated_metrics.get('total_stages', num_stages_from_plan)
                shuffle_read = aggregated_metrics.get('total_shuffle_read_bytes', 0)
                shuffle_write = aggregated_metrics.get('total_shuffle_write_bytes', 0)
                total_input_rows = aggregated_metrics.get('total_input_records', 0)
                total_output_rows = aggregated_metrics.get('total_output_records', 0)
                stages = real_stage_metrics
                logger.info(f"Using real stage metrics: {num_stages} stages, shuffle_read={shuffle_read} bytes, shuffle_write={shuffle_write} bytes, {total_input_rows} input rows, {total_output_rows} output rows")
            else:
                # No real metrics available - use zeros and warn
                num_stages = num_stages_from_plan
                shuffle_read = 0
                shuffle_write = 0
                total_input_rows = 0
                total_output_rows = 0
                stages = []
                logger.warning(f"Real stage metrics unavailable - showing zeros. Enable Spark metrics collection to see actual values.")

            # Calculate intermediate result size
            intermediate_size = shuffle_write + shuffle_read

            # Collect operator-level metrics from the physical plan tree
            operator_metrics = []
            if self.operator_metrics_collector and plan_tree is not None:
                try:
                    logger.debug("Collecting operator metrics from physical plan tree")
                    operator_metrics = self.operator_metrics_collector.collect_operator_metrics_from_plan(plan_tree)
                    logger.info(f"Collected metrics for {len(operator_metrics)} operators")
                except Exception as e:
                    logger.warning(f"Could not collect operator metrics: {e}", exc_info=True)
            else:
                if not self.operator_metrics_collector:
                    logger.debug("Operator metrics collector is not initialized")
                if plan_tree is None:
                    logger.debug("No plan tree available for operator metrics collection")

            return ExecutionMetrics(
                execution_time_ms=execution_time,
                planning_time_ms=planning_time,
                num_stages=num_stages,
                total_input_rows=total_input_rows,
                total_output_rows=total_output_rows,
                intermediate_result_size_bytes=intermediate_size,
                avoided_materialization_bytes=0,  # Will be calculated by comparing ref vs opt
                # Detailed metrics
                operator_counts=operator_counts,
                shuffle_read_bytes=shuffle_read,
                shuffle_write_bytes=shuffle_write,
                num_tasks=detailed_metrics.get('num_tasks', 0),
                stages=stages,
                operator_metrics=operator_metrics
            )

        except Exception as e:
            logger.warning(f"Could not extract detailed metrics: {e}", exc_info=True)
            return ExecutionMetrics(
                execution_time_ms=execution_time,
                planning_time_ms=planning_time
            )

    def _apply_spark_config(self, config: SparkConfig):
        """Apply Spark SQL configuration options"""
        if not self.spark:
            raise RuntimeError("Spark not initialized")

        # Apply Yannakakis optimization flags
        yannakakis_val = "true" if config.yannakakis_enabled else "false"
        physical_count_val = "true" if config.physical_count_join_enabled else "false"
        unguarded_val = "true" if config.unguarded_enabled else "false"

        self.spark.conf.set("spark.sql.yannakakis.enabled", yannakakis_val)
        self.spark.conf.set("spark.sql.yannakakis.physicalCountJoinEnabled", physical_count_val)
        self.spark.conf.set("spark.sql.yannakakis.unguardedEnabled", unguarded_val)

        logger.info(f"Applied Spark config: yannakakis={yannakakis_val}, physicalCountJoin={physical_count_val}, unguarded={unguarded_val}")

        # Apply any custom options
        if config.custom_options:
            for key, value in config.custom_options.items():
                self.spark.conf.set(key, value)
                logger.info(f"Applied custom config: {key}={value}")

    def _detect_optimizations(self, plan: str) -> list[str]:
        """Detect which optimizations were applied from plan string"""
        optimizations = []

        # Look for specific patterns in the physical plan
        if "AvoidMaterialization" in plan:
            optimizations.append("Avoided intermediate materialization")

        if "CountJoin" in plan:
            optimizations.append("Count-join optimization")

        if "PushdownAggregate" in plan:
            optimizations.append("Aggregate pushdown")

        return optimizations

    def _build_plan_tree_from_operator(self, operator) -> Optional[Dict[str, Any]]:
        """
        Build a plan tree dictionary by traversing the actual Spark operator structure

        This method walks the physical plan operator tree directly, preserving
        the hierarchical parent-child relationships instead of relying on JSON
        serialization which may return a flat list.

        Args:
            operator: Spark physical plan operator (Java object)

        Returns:
            Dictionary representation of the operator tree with nested children
        """
        try:
            # Get operator class name
            class_name = operator.getClass().getName()

            # Unwrap AdaptiveSparkPlanExec to get the actual executed plan
            # AQE wraps the plan but we want the real operators inside
            if "AdaptiveSparkPlanExec" in class_name:
                try:
                    # Try to get the final physical plan from AQE
                    # Different Spark versions may have different methods
                    if hasattr(operator, 'executedPlan'):
                        inner_plan = operator.executedPlan()
                        logger.debug("Unwrapped AdaptiveSparkPlanExec using executedPlan()")
                        return self._build_plan_tree_from_operator(inner_plan)
                    elif hasattr(operator, 'finalPhysicalPlan'):
                        inner_plan = operator.finalPhysicalPlan()
                        logger.debug("Unwrapped AdaptiveSparkPlanExec using finalPhysicalPlan()")
                        return self._build_plan_tree_from_operator(inner_plan)
                except Exception as e:
                    logger.debug(f"Could not unwrap AdaptiveSparkPlanExec: {e}")
                    # Fall through to normal processing

            # Note: We do NOT unwrap WholeStageCodegenExec because it represents
            # a fused operator group and traversing its children() will give us
            # the actual underlying operators

            # Unwrap AQE shuffle readers and query stages to get to the actual operators
            # These are added by AQE but aren't the "real" operators we want to show
            if any(x in class_name for x in ["AQEShuffleReadExec", "CustomShuffleReaderExec",
                                               "ShuffleQueryStageExec", "BroadcastQueryStageExec"]):
                try:
                    # These have a child() or plan() method
                    if hasattr(operator, 'plan'):
                        inner_plan = operator.plan()
                        logger.debug(f"Unwrapped {class_name.split('.')[-1]} using plan()")
                        return self._build_plan_tree_from_operator(inner_plan)
                    elif hasattr(operator, 'child'):
                        inner_plan = operator.child()
                        logger.debug(f"Unwrapped {class_name.split('.')[-1]} using child()")
                        return self._build_plan_tree_from_operator(inner_plan)
                except Exception as e:
                    logger.debug(f"Could not unwrap AQE operator: {e}")
                    # Fall through to normal processing

            # Get simple string representation
            # Note: simpleString is a Scala field (not a method) and can't be accessed directly via py4j
            # Use toString() instead and extract the first line
            try:
                full_string = operator.toString()
                # Get first line only (simpleString equivalent)
                simple_string = full_string.split('\n')[0] if full_string else "Unknown"
            except Exception as e:
                logger.debug(f"Could not get operator string: {e}")
                simple_string = operator.getClass().getName().split('.')[-1]

            # Extract metrics from the operator
            metrics_list = []
            try:
                # Spark operators have a metrics() method that returns Map[String, SQLMetric]
                # This is a Scala Map, not a Java Map, so use iterator() directly
                metrics_map = operator.metrics()
                logger.debug(f"Operator {class_name.split('.')[-1]}: metrics_map type = {type(metrics_map)}")

                # Convert Scala map to Python dict
                # Scala Maps use iterator() directly (not entrySet().iterator() like Java Maps)
                if metrics_map and hasattr(metrics_map, 'iterator'):
                    iterator = metrics_map.iterator()
                    entry_count = 0
                    while iterator.hasNext():
                        # Each entry is a Tuple2 in Scala
                        tuple2 = iterator.next()
                        entry_count += 1

                        # Access tuple elements: _1 for key, _2 for value
                        metric_name = tuple2._1()
                        metric_obj = tuple2._2()
                        logger.debug(f"  Metric entry {entry_count}: name={metric_name}, obj type={type(metric_obj)}")

                        # Get the metric value - SQLMetric has a value() method
                        try:
                            metric_value = metric_obj.value() if hasattr(metric_obj, 'value') else 0
                            logger.debug(f"    Value: {metric_value}")
                            metrics_list.append({
                                'name': str(metric_name),
                                'value': int(metric_value) if metric_value else 0
                            })
                        except Exception as e:
                            logger.debug(f"    Could not get value: {e}")

                    logger.debug(f"Extracted {len(metrics_list)} metrics from operator {class_name.split('.')[-1]}")
                else:
                    logger.debug(f"Operator {class_name.split('.')[-1]}: metrics_map is None or missing iterator method")

            except Exception as e:
                logger.debug(f"Could not extract metrics from operator: {e}")

            # Build node dictionary
            node = {
                "class": class_name,
                "simpleString": simple_string,
                "metrics": metrics_list,
                "children": []
            }

            # Recursively process children
            # Spark operators have a children() method that returns Seq[SparkPlan]
            try:
                children_seq = operator.children()
                # Scala Seq is not directly iterable in Python, use apply() indexing
                num_children = children_seq.size()

                for i in range(num_children):
                    child_operator = children_seq.apply(i)
                    child_node = self._build_plan_tree_from_operator(child_operator)
                    if child_node:
                        node["children"].append(child_node)

            except Exception as e:
                logger.debug(f"No children or error accessing children: {e}")

            return node

        except Exception as e:
            logger.warning(f"Error building plan tree from operator: {e}", exc_info=True)
            return None

    async def get_table_stats(self, table_name: str) -> dict:
        """Get statistics for a table"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self._get_table_stats_sync,
            table_name
        )

    def _get_table_stats_sync(self, table_name: str) -> dict:
        """Get table statistics synchronously"""
        if not self.spark:
            raise RuntimeError("Spark not initialized")

        df = self.spark.table(table_name)
        return {
            "row_count": df.count(),
            "columns": df.columns,
            "schema": df.schema.simpleString()
        }

    def get_table_count(self) -> int:
        """Get the number of tables registered in Spark"""
        if not self.spark:
            return 0
        try:
            tables = self.spark.catalog.listTables()
            return len(tables)
        except Exception as e:
            logger.warning(f"Error getting table count: {e}")
            return 0

    def get_database_info(self) -> Optional[Dict[str, Any]]:
        """Get current database information if connected to PostgreSQL"""
        if self.current_database:
            return {
                "type": "postgresql",
                "database": self.current_database
            }
        return None

    def set_current_database(self, database: str):
        """Set the current connected database name"""
        self.current_database = database
