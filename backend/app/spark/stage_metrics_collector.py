"""
Stage-level metrics collection from Spark execution

This module provides functionality to collect actual runtime metrics
for each stage of a Spark query execution.
"""
import logging
from typing import Dict, List, Any, Optional
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class StageMetricsCollector:
    """
    Collects actual stage-level metrics from Spark execution

    Uses SparkContext's StatusTracker to retrieve completed stage information
    including timing, I/O, and task metrics.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.sc = spark.sparkContext
        self.status_tracker = self.sc.statusTracker()

    def collect_stage_metrics(self, job_ids: List[int]) -> List[Dict[str, Any]]:
        """
        Collect metrics for all stages in the given job IDs

        Args:
            job_ids: List of Spark job IDs to collect metrics for

        Returns:
            List of stage metrics dictionaries
        """
        stage_metrics = []

        try:
            # Get all stage IDs for the jobs
            all_stage_ids = []
            for job_id in job_ids:
                job_info = self.status_tracker.getJobInfo(job_id)
                if job_info:
                    all_stage_ids.extend(job_info.stageIds())

            logger.debug(f"Collecting metrics for {len(all_stage_ids)} stages from {len(job_ids)} jobs")

            # Collect metrics for each stage
            for stage_id in all_stage_ids:
                stage_info = self.status_tracker.getStageInfo(stage_id)

                if stage_info:
                    metrics = self._extract_stage_metrics(stage_id, stage_info)
                    if metrics:
                        stage_metrics.append(metrics)

            logger.info(f"Successfully collected metrics for {len(stage_metrics)} stages")

        except Exception as e:
            logger.warning(f"Error collecting stage metrics: {e}", exc_info=True)

        return stage_metrics

    def _extract_stage_metrics(self, stage_id: int, stage_info) -> Optional[Dict[str, Any]]:
        """
        Extract metrics from a single stage

        Args:
            stage_id: Spark stage ID
            stage_info: StageInfo object from StatusTracker

        Returns:
            Dictionary of stage metrics or None if unavailable
        """
        try:
            # Get task metrics by accessing the Spark UI's internal data
            # Note: This uses internal Spark APIs which may vary by version
            stage_data = self._get_stage_data_from_ui(stage_id)

            if not stage_data:
                # Fallback to basic info from StatusTracker
                return {
                    'stage_id': stage_id,
                    'name': stage_info.name(),
                    'num_tasks': stage_info.numTasks(),
                    'num_completed_tasks': stage_info.numCompletedTasks(),
                    'num_failed_tasks': stage_info.numFailedTasks(),
                    'status': 'completed' if stage_info.numCompletedTasks() == stage_info.numTasks() else 'partial',
                    # Estimated metrics (fallback)
                    'execution_time_ms': 0,
                    'input_records': 0,
                    'output_records': 0,
                    'shuffle_read_bytes': 0,
                    'shuffle_write_bytes': 0,
                    'memory_bytes_spilled': 0,
                    'disk_bytes_spilled': 0
                }

            # Extract detailed metrics from stage data
            metrics = {
                'stage_id': stage_id,
                'name': stage_data.get('name', ''),
                'num_tasks': stage_data.get('numTasks', 0),
                'num_completed_tasks': stage_data.get('numCompleteTasks', 0),
                'num_failed_tasks': stage_data.get('numFailedTasks', 0),
                'status': stage_data.get('status', 'unknown').lower(),

                # Timing metrics (in milliseconds)
                'submission_time': stage_data.get('submissionTime', 0),
                'completion_time': stage_data.get('completionTime', 0),
                'execution_time_ms': self._calculate_execution_time(stage_data),

                # I/O metrics
                'input_records': self._get_metric(stage_data, 'inputRecords', 0),
                'input_bytes': self._get_metric(stage_data, 'inputBytes', 0),
                'output_records': self._get_metric(stage_data, 'outputRecords', 0),
                'output_bytes': self._get_metric(stage_data, 'outputBytes', 0),

                # Shuffle metrics
                'shuffle_read_records': self._get_metric(stage_data, 'shuffleReadRecords', 0),
                'shuffle_read_bytes': self._get_metric(stage_data, 'shuffleReadBytes', 0),
                'shuffle_write_records': self._get_metric(stage_data, 'shuffleWriteRecords', 0),
                'shuffle_write_bytes': self._get_metric(stage_data, 'shuffleWriteBytes', 0),

                # Memory and disk spill
                'memory_bytes_spilled': self._get_metric(stage_data, 'memoryBytesSpilled', 0),
                'disk_bytes_spilled': self._get_metric(stage_data, 'diskBytesSpilled', 0),

                # Task time breakdown (in milliseconds)
                'executor_run_time': self._get_metric(stage_data, 'executorRunTime', 0),
                'executor_cpu_time': self._get_metric(stage_data, 'executorCpuTime', 0) // 1_000_000,  # Convert ns to ms
                'jvm_gc_time': self._get_metric(stage_data, 'jvmGcTime', 0),
            }

            return metrics

        except Exception as e:
            logger.debug(f"Error extracting metrics for stage {stage_id}: {e}")
            return None

    def _get_stage_data_from_ui(self, stage_id: int) -> Optional[Dict]:
        """
        Get stage data from Spark UI's internal storage

        This attempts to access the UI's stage data which contains detailed metrics.
        Falls back gracefully if unavailable.
        """
        try:
            # Access Spark's internal UI storage
            # This uses private APIs but is the most reliable way to get detailed metrics
            sc = self.sc

            # Try to get the stage data from the AppStatusStore
            if hasattr(sc, '_jsc'):
                # Access Java SparkContext
                jsc = sc._jsc
                jvm = sc._jvm

                # Get the AppStatusStore (Spark 2.3+)
                if hasattr(jvm.org.apache.spark, 'status'):
                    try:
                        # Get the live UI if available
                        spark_ui = jsc.sc().ui()
                        if spark_ui.isDefined():
                            ui = spark_ui.get()
                            store = ui.store()

                            # Get stage data
                            stage_data_list = store.stageList(None)

                            # Find our stage
                            for stage_wrapper in stage_data_list:
                                stage_data = stage_wrapper
                                if stage_data.stageId() == stage_id:
                                    # Convert to dictionary
                                    return self._convert_stage_data_to_dict(stage_data)
                    except Exception as e:
                        logger.debug(f"Could not access AppStatusStore: {e}")

        except Exception as e:
            logger.debug(f"Error accessing UI data for stage {stage_id}: {e}")

        return None

    def _convert_stage_data_to_dict(self, stage_data) -> Dict:
        """Convert Java StageData object to Python dictionary"""
        try:
            # Extract metrics from the StageData object
            metrics = {}

            # Basic info
            metrics['stage_id'] = stage_data.stageId()
            metrics['name'] = str(stage_data.name()) if hasattr(stage_data, 'name') else ''
            metrics['status'] = str(stage_data.status()) if hasattr(stage_data, 'status') else 'unknown'
            metrics['numTasks'] = stage_data.numTasks() if hasattr(stage_data, 'numTasks') else 0
            metrics['numCompleteTasks'] = stage_data.numCompleteTasks() if hasattr(stage_data, 'numCompleteTasks') else 0
            metrics['numFailedTasks'] = stage_data.numFailedTasks() if hasattr(stage_data, 'numFailedTasks') else 0

            # Timing
            if hasattr(stage_data, 'submissionTime'):
                sub_time = stage_data.submissionTime()
                metrics['submissionTime'] = sub_time.get() if hasattr(sub_time, 'get') else 0

            if hasattr(stage_data, 'completionTime'):
                comp_time = stage_data.completionTime()
                metrics['completionTime'] = comp_time.get() if hasattr(comp_time, 'get') else 0

            # Task metrics
            if hasattr(stage_data, 'taskMetrics'):
                task_metrics = stage_data.taskMetrics()
                if task_metrics:
                    metrics['executorRunTime'] = task_metrics.executorRunTime() if hasattr(task_metrics, 'executorRunTime') else 0
                    metrics['executorCpuTime'] = task_metrics.executorCpuTime() if hasattr(task_metrics, 'executorCpuTime') else 0
                    metrics['jvmGcTime'] = task_metrics.jvmGcTime() if hasattr(task_metrics, 'jvmGcTime') else 0
                    metrics['memoryBytesSpilled'] = task_metrics.memoryBytesSpilled() if hasattr(task_metrics, 'memoryBytesSpilled') else 0
                    metrics['diskBytesSpilled'] = task_metrics.diskBytesSpilled() if hasattr(task_metrics, 'diskBytesSpilled') else 0

                    # Input metrics
                    if hasattr(task_metrics, 'inputMetrics'):
                        input_metrics = task_metrics.inputMetrics()
                        if input_metrics:
                            metrics['inputRecords'] = input_metrics.recordsRead() if hasattr(input_metrics, 'recordsRead') else 0
                            metrics['inputBytes'] = input_metrics.bytesRead() if hasattr(input_metrics, 'bytesRead') else 0

                    # Output metrics
                    if hasattr(task_metrics, 'outputMetrics'):
                        output_metrics = task_metrics.outputMetrics()
                        if output_metrics:
                            metrics['outputRecords'] = output_metrics.recordsWritten() if hasattr(output_metrics, 'recordsWritten') else 0
                            metrics['outputBytes'] = output_metrics.bytesWritten() if hasattr(output_metrics, 'bytesWritten') else 0

                    # Shuffle read metrics
                    if hasattr(task_metrics, 'shuffleReadMetrics'):
                        shuffle_read = task_metrics.shuffleReadMetrics()
                        if shuffle_read:
                            metrics['shuffleReadRecords'] = shuffle_read.recordsRead() if hasattr(shuffle_read, 'recordsRead') else 0
                            metrics['shuffleReadBytes'] = shuffle_read.totalBytesRead() if hasattr(shuffle_read, 'totalBytesRead') else 0

                    # Shuffle write metrics
                    if hasattr(task_metrics, 'shuffleWriteMetrics'):
                        shuffle_write = task_metrics.shuffleWriteMetrics()
                        if shuffle_write:
                            metrics['shuffleWriteRecords'] = shuffle_write.recordsWritten() if hasattr(shuffle_write, 'recordsWritten') else 0
                            metrics['shuffleWriteBytes'] = shuffle_write.bytesWritten() if hasattr(shuffle_write, 'bytesWritten') else 0

            return metrics

        except Exception as e:
            logger.debug(f"Error converting stage data: {e}")
            return {}

    def _calculate_execution_time(self, stage_data: Dict) -> int:
        """Calculate stage execution time in milliseconds"""
        submission = stage_data.get('submissionTime', 0)
        completion = stage_data.get('completionTime', 0)

        if submission > 0 and completion > 0:
            return completion - submission

        # Fallback to executor run time
        return stage_data.get('executorRunTime', 0)

    def _get_metric(self, stage_data: Dict, metric_name: str, default: Any = 0) -> Any:
        """Safely get a metric from stage data with fallback"""
        return stage_data.get(metric_name, default)

    def aggregate_metrics_by_stage(self, stage_metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Aggregate stage metrics into overall query metrics

        Args:
            stage_metrics: List of per-stage metrics

        Returns:
            Aggregated metrics dictionary
        """
        if not stage_metrics:
            return {
                'total_stages': 0,
                'total_execution_time_ms': 0,
                'total_input_records': 0,
                'total_output_records': 0,
                'total_shuffle_read_bytes': 0,
                'total_shuffle_write_bytes': 0,
                'total_memory_spilled': 0,
                'total_disk_spilled': 0,
                'stages': []
            }

        # Aggregate metrics
        total_execution_time = sum(s.get('execution_time_ms', 0) for s in stage_metrics)
        total_input_records = sum(s.get('input_records', 0) for s in stage_metrics)
        total_output_records = sum(s.get('output_records', 0) for s in stage_metrics)
        total_shuffle_read = sum(s.get('shuffle_read_bytes', 0) for s in stage_metrics)
        total_shuffle_write = sum(s.get('shuffle_write_bytes', 0) for s in stage_metrics)
        total_memory_spilled = sum(s.get('memory_bytes_spilled', 0) for s in stage_metrics)
        total_disk_spilled = sum(s.get('disk_bytes_spilled', 0) for s in stage_metrics)

        return {
            'total_stages': len(stage_metrics),
            'total_execution_time_ms': total_execution_time,
            'total_input_records': total_input_records,
            'total_output_records': total_output_records,
            'total_shuffle_read_bytes': total_shuffle_read,
            'total_shuffle_write_bytes': total_shuffle_write,
            'total_memory_spilled': total_memory_spilled,
            'total_disk_spilled': total_disk_spilled,
            'stages': stage_metrics
        }
