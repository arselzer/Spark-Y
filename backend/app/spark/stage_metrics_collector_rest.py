"""
REST API-based stage metrics collector for Spark
Uses Spark's official REST API instead of internal Java interop
"""
import logging
import requests
from typing import List, Dict, Any, Optional
from urllib.parse import urlsplit, urlunsplit
from datetime import datetime

logger = logging.getLogger(__name__)


class StageMetricsCollectorREST:
    """
    Collects stage-level metrics using Spark's REST API

    Uses the official Spark monitoring REST API which is stable across versions.
    Requires Spark UI to be enabled (default).
    """

    def __init__(self, spark):
        """
        Initialize REST API collector

        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        self.sc = spark.sparkContext
        self.app_id = None
        self.api_url = None
        self._initialize_api_connection()

    def _initialize_api_connection(self):
        """Initialize connection to Spark REST API"""
        try:
            # Get application ID (trivial!)
            self.app_id = self.sc.applicationId

            # Get UI URL and construct API URL
            ui_web_url = self.sc.uiWebUrl
            if ui_web_url:
                # Parse URL and replace host with localhost if needed
                # (in case UI is bound to 0.0.0.0 but we need to connect via localhost)
                parsed = list(urlsplit(ui_web_url))
                host_port = parsed[1]
                # Keep the port, change host to localhost
                parsed[1] = 'localhost' + host_port[host_port.find(':'):]
                self.api_url = f'{urlunsplit(parsed)}/api/v1'

                logger.info(f"Initialized REST API collector: {self.api_url}, app_id: {self.app_id}")
            else:
                logger.warning("Spark UI URL not available, REST API collection will fail")

        except Exception as e:
            logger.warning(f"Failed to initialize REST API connection: {e}", exc_info=True)

    def get_jobs_by_group(self, job_group_id: str) -> List[int]:
        """
        Get all job IDs that belong to a specific job group using REST API

        Args:
            job_group_id: The job group ID to filter by

        Returns:
            List of job IDs belonging to this job group
        """
        if not self.api_url or not self.app_id:
            logger.warning("REST API not initialized, cannot get jobs by group")
            return []

        try:
            # Get all jobs for this application
            jobs_url = f"{self.api_url}/applications/{self.app_id}/jobs"
            response = requests.get(jobs_url, timeout=5)

            if response.status_code != 200:
                logger.warning(f"Failed to get jobs list: HTTP {response.status_code}")
                return []

            jobs = response.json()
            matching_job_ids = []

            # Filter jobs by job group
            for job in jobs:
                # The job group is stored in the 'jobGroup' field
                if job.get('jobGroup') == job_group_id:
                    matching_job_ids.append(job['jobId'])

            logger.info(f"Found {len(matching_job_ids)} jobs matching job group '{job_group_id}'")
            return matching_job_ids

        except Exception as e:
            logger.warning(f"Error getting jobs by group: {e}", exc_info=True)
            return []

    def collect_stage_metrics(self, job_ids: List[int]) -> List[Dict[str, Any]]:
        """
        Collect metrics for all stages in the given job IDs using REST API

        Args:
            job_ids: List of Spark job IDs to collect metrics for

        Returns:
            List of stage metrics dictionaries
        """
        if not self.api_url or not self.app_id:
            logger.warning("REST API not initialized, cannot collect metrics")
            return []

        stage_metrics = []

        try:
            # Collect all unique stage IDs from the jobs
            all_stage_ids = set()

            for job_id in job_ids:
                job_url = f"{self.api_url}/applications/{self.app_id}/jobs/{job_id}"
                try:
                    response = requests.get(job_url, timeout=5)
                    if response.status_code == 200:
                        job_data = response.json()
                        all_stage_ids.update(job_data.get('stageIds', []))
                    else:
                        logger.debug(f"Job {job_id} not found (status {response.status_code})")
                except requests.RequestException as e:
                    logger.debug(f"Error fetching job {job_id}: {e}")

            logger.debug(f"Found {len(all_stage_ids)} stages from {len(job_ids)} jobs")

            # Collect metrics for each stage
            for stage_id in all_stage_ids:
                stage_data = self._get_stage_metrics(stage_id)
                if stage_data:
                    stage_metrics.append(stage_data)

            logger.info(f"Collected REST API metrics for {len(stage_metrics)} stages")

        except Exception as e:
            logger.warning(f"Error collecting stage metrics via REST API: {e}", exc_info=True)

        return stage_metrics

    def _get_stage_metrics(self, stage_id: int) -> Optional[Dict[str, Any]]:
        """
        Get detailed metrics for a single stage

        Args:
            stage_id: Spark stage ID

        Returns:
            Dictionary with stage metrics, or None if stage not found
        """
        try:
            # Get stage list (may have multiple attempts)
            stage_list_url = f"{self.api_url}/applications/{self.app_id}/stages/{stage_id}"
            response = requests.get(stage_list_url, timeout=5)

            if response.status_code != 200:
                logger.debug(f"Stage {stage_id} not found (status {response.status_code})")
                return None

            stage_attempts = response.json()

            # Use the latest attempt (last in list)
            if not stage_attempts:
                return None

            stage = stage_attempts[-1]  # Most recent attempt
            attempt_id = stage.get('attemptId', 0)

            # Get detailed metrics for this specific attempt
            detail_url = f"{self.api_url}/applications/{self.app_id}/stages/{stage_id}/{attempt_id}"
            detail_params = {'details': 'true', 'withSummaries': 'true'}
            detail_response = requests.get(detail_url, params=detail_params, timeout=5)

            if detail_response.status_code != 200:
                logger.debug(f"Stage {stage_id}/{attempt_id} details not found")
                return None

            detail = detail_response.json()

            # Extract metrics
            metrics = self._extract_metrics_from_stage_data(stage_id, detail)
            return metrics

        except Exception as e:
            logger.debug(f"Error getting metrics for stage {stage_id}: {e}")
            return None

    def _extract_metrics_from_stage_data(self, stage_id: int, stage_data: Dict) -> Dict[str, Any]:
        """
        Extract metrics from REST API stage data

        Args:
            stage_id: Stage ID
            stage_data: Stage data from REST API

        Returns:
            Normalized metrics dictionary
        """
        # Parse timestamps
        submission_time = self._parse_timestamp(stage_data.get('submissionTime'))
        completion_time = self._parse_timestamp(stage_data.get('completionTime'))

        # Calculate execution time
        execution_time_ms = 0
        if submission_time and completion_time:
            execution_time_ms = completion_time - submission_time

        # Extract metrics
        metrics = {
            'stage_id': stage_id,
            'name': stage_data.get('name', ''),
            'status': stage_data.get('status', 'UNKNOWN'),
            'num_tasks': stage_data.get('numTasks', 0),
            'submission_time': submission_time,
            'completion_time': completion_time,
            'execution_time_ms': execution_time_ms,

            # Input metrics
            'input_records': stage_data.get('inputRecords', 0),
            'input_bytes': stage_data.get('inputBytes', 0),

            # Output metrics
            'output_records': stage_data.get('outputRecords', 0),
            'output_bytes': stage_data.get('outputBytes', 0),

            # Shuffle read metrics
            'shuffle_read_records': stage_data.get('shuffleReadRecords', 0),
            'shuffle_read_bytes': stage_data.get('shuffleReadBytes', 0),

            # Shuffle write metrics
            'shuffle_write_records': stage_data.get('shuffleWriteRecords', 0),
            'shuffle_write_bytes': stage_data.get('shuffleWriteBytes', 0),

            # Spill metrics
            'memory_bytes_spilled': stage_data.get('memoryBytesSpilled', 0),
            'disk_bytes_spilled': stage_data.get('diskBytesSpilled', 0),

            # CPU metrics
            'executor_run_time': stage_data.get('executorRunTime', 0),
            'executor_cpu_time': stage_data.get('executorCpuTime', 0),
            'jvm_gc_time': stage_data.get('jvmGcTime', 0)
        }

        return metrics

    def _parse_timestamp(self, timestamp_str: Optional[str]) -> Optional[int]:
        """
        Parse timestamp string to milliseconds since epoch

        Args:
            timestamp_str: Timestamp string from REST API (e.g., "2025-01-19T10:30:15.123GMT")

        Returns:
            Milliseconds since epoch, or None if parsing fails
        """
        if not timestamp_str:
            return None

        try:
            # Spark REST API returns timestamps like "2025-01-19T10:30:15.123GMT"
            # Remove GMT suffix and parse
            if timestamp_str.endswith('GMT'):
                timestamp_str = timestamp_str[:-3]

            # Parse ISO format
            dt = datetime.fromisoformat(timestamp_str)

            # Convert to milliseconds since epoch
            return int(dt.timestamp() * 1000)

        except Exception as e:
            logger.debug(f"Error parsing timestamp '{timestamp_str}': {e}")
            return None

    def aggregate_metrics_by_stage(self, stage_metrics: List[Dict]) -> Dict[str, Any]:
        """
        Aggregate individual stage metrics into overall query metrics

        Args:
            stage_metrics: List of stage metric dictionaries

        Returns:
            Aggregated metrics dictionary
        """
        if not stage_metrics:
            return {}

        aggregated = {
            'total_stages': len(stage_metrics),
            'total_shuffle_read_bytes': sum(s.get('shuffle_read_bytes', 0) for s in stage_metrics),
            'total_shuffle_write_bytes': sum(s.get('shuffle_write_bytes', 0) for s in stage_metrics),
            'total_input_records': sum(s.get('input_records', 0) for s in stage_metrics),
            'total_output_records': sum(s.get('output_records', 0) for s in stage_metrics),
            'total_input_bytes': sum(s.get('input_bytes', 0) for s in stage_metrics),
            'total_output_bytes': sum(s.get('output_bytes', 0) for s in stage_metrics),
            'total_memory_spilled': sum(s.get('memory_bytes_spilled', 0) for s in stage_metrics),
            'total_disk_spilled': sum(s.get('disk_bytes_spilled', 0) for s in stage_metrics),
            'total_executor_run_time': sum(s.get('executor_run_time', 0) for s in stage_metrics),
            'total_executor_cpu_time': sum(s.get('executor_cpu_time', 0) for s in stage_metrics),
            'total_jvm_gc_time': sum(s.get('jvm_gc_time', 0) for s in stage_metrics)
        }

        return aggregated
