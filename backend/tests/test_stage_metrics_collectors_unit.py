"""
Unit tests for stage metrics collectors (no PySpark required)

These tests run without requiring PySpark installation by mocking all dependencies.
Can be run locally for quick validation.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import requests


class TestRESTAPICollectorUnit:
    """Unit tests for REST API collector without PySpark dependency"""

    @patch('app.spark.stage_metrics_collector_rest.StageMetricsCollectorREST.__init__')
    def test_parse_timestamp_valid(self, mock_init):
        """Test timestamp parsing with valid Spark format"""
        # Import after patch to avoid import errors
        from app.spark.stage_metrics_collector_rest import StageMetricsCollectorREST

        # Mock __init__ to do nothing (avoid SparkSession requirement)
        mock_init.return_value = None

        collector = StageMetricsCollectorREST(None)

        # Manually call the method we want to test
        timestamp_str = "2025-01-19T10:30:15.123GMT"
        result = collector._parse_timestamp(timestamp_str)

        assert result is not None
        assert isinstance(result, int)
        assert result > 0
        # Should be approximately 2025-01-19 10:30:15 UTC in milliseconds
        # 1737283815123 milliseconds since epoch

    @patch('app.spark.stage_metrics_collector_rest.StageMetricsCollectorREST.__init__')
    def test_parse_timestamp_none(self, mock_init):
        """Test timestamp parsing with None input"""
        from app.spark.stage_metrics_collector_rest import StageMetricsCollectorREST

        mock_init.return_value = None
        collector = StageMetricsCollectorREST(None)

        result = collector._parse_timestamp(None)
        assert result is None

    @patch('app.spark.stage_metrics_collector_rest.StageMetricsCollectorREST.__init__')
    def test_parse_timestamp_empty(self, mock_init):
        """Test timestamp parsing with empty string"""
        from app.spark.stage_metrics_collector_rest import StageMetricsCollectorREST

        mock_init.return_value = None
        collector = StageMetricsCollectorREST(None)

        result = collector._parse_timestamp("")
        assert result is None

    @patch('app.spark.stage_metrics_collector_rest.StageMetricsCollectorREST.__init__')
    def test_extract_metrics_from_stage_data(self, mock_init):
        """Test extracting metrics from stage data dict"""
        from app.spark.stage_metrics_collector_rest import StageMetricsCollectorREST

        mock_init.return_value = None
        collector = StageMetricsCollectorREST(None)

        stage_data = {
            'stageId': 5,
            'attemptId': 0,
            'name': 'Exchange hashpartitioning',
            'status': 'COMPLETE',
            'numTasks': 20,
            'submissionTime': '2025-01-19T10:30:00.000GMT',
            'completionTime': '2025-01-19T10:30:10.000GMT',
            'inputRecords': 5000,
            'inputBytes': 100000,
            'outputRecords': 5000,
            'outputBytes': 100000,
            'shuffleReadBytes': 50000,
            'shuffleWriteBytes': 50000,
            'memoryBytesSpilled': 1024,
            'diskBytesSpilled': 0,
            'executorRunTime': 10000,
            'executorCpuTime': 9500,
            'jvmGcTime': 200
        }

        metrics = collector._extract_metrics_from_stage_data(5, stage_data)

        assert metrics['stage_id'] == 5
        assert metrics['name'] == 'Exchange hashpartitioning'
        assert metrics['num_tasks'] == 20
        assert metrics['execution_time_ms'] == 10000  # 10 seconds
        assert metrics['input_records'] == 5000
        assert metrics['shuffle_read_bytes'] == 50000
        assert metrics['memory_bytes_spilled'] == 1024

    @patch('requests.get')
    @patch('app.spark.stage_metrics_collector_rest.StageMetricsCollectorREST.__init__')
    def test_collect_stage_metrics_with_mocked_api(self, mock_init, mock_get):
        """Test collecting stage metrics with mocked REST API responses"""
        from app.spark.stage_metrics_collector_rest import StageMetricsCollectorREST

        mock_init.return_value = None
        collector = StageMetricsCollectorREST(None)
        collector.app_id = "test-app-id"
        collector.api_url = "http://localhost:4040/api/v1"

        # Mock job response
        job_response = Mock()
        job_response.status_code = 200
        job_response.json.return_value = {
            'jobId': 0,
            'stageIds': [0, 1]
        }

        # Mock stage detail response
        stage_response = Mock()
        stage_response.status_code = 200
        stage_response.json.return_value = {
            'stageId': 0,
            'attemptId': 0,
            'name': 'WholeStageCodegen (1)',
            'status': 'COMPLETE',
            'numTasks': 10,
            'submissionTime': '2025-01-19T10:30:00.000GMT',
            'completionTime': '2025-01-19T10:30:05.500GMT',
            'inputRecords': 1000,
            'inputBytes': 50000,
            'outputRecords': 100,
            'outputBytes': 5000,
            'shuffleReadBytes': 10000,
            'shuffleWriteBytes': 8000,
            'memoryBytesSpilled': 0,
            'diskBytesSpilled': 0,
            'executorRunTime': 5000,
            'executorCpuTime': 4500,
            'jvmGcTime': 100
        }

        # Configure mock to return different responses
        mock_get.side_effect = [job_response, stage_response, stage_response]

        # Collect metrics
        metrics = collector.collect_stage_metrics([0])

        # Verify results
        assert len(metrics) == 2  # 2 stages
        assert metrics[0]['stage_id'] == 0
        assert metrics[0]['name'] == 'WholeStageCodegen (1)'
        assert metrics[0]['num_tasks'] == 10
        assert metrics[0]['execution_time_ms'] == 5500  # 5.5 seconds
        assert metrics[0]['shuffle_read_bytes'] == 10000
        assert metrics[0]['shuffle_write_bytes'] == 8000


class TestErrorHandlingUnit:
    """Unit tests for error handling without PySpark dependency"""

    @patch('requests.get')
    @patch('app.spark.stage_metrics_collector_rest.StageMetricsCollectorREST.__init__')
    def test_rest_collector_handles_network_error(self, mock_init, mock_get):
        """Test REST collector handles network errors gracefully"""
        from app.spark.stage_metrics_collector_rest import StageMetricsCollectorREST

        mock_init.return_value = None
        collector = StageMetricsCollectorREST(None)
        collector.app_id = "test-app-id"
        collector.api_url = "http://localhost:4040/api/v1"

        # Simulate network error
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection failed")

        # Should return empty list, not crash
        metrics = collector.collect_stage_metrics([0])
        assert metrics == []

    @patch('requests.get')
    @patch('app.spark.stage_metrics_collector_rest.StageMetricsCollectorREST.__init__')
    def test_rest_collector_handles_timeout(self, mock_init, mock_get):
        """Test REST collector handles timeout gracefully"""
        from app.spark.stage_metrics_collector_rest import StageMetricsCollectorREST

        mock_init.return_value = None
        collector = StageMetricsCollectorREST(None)
        collector.app_id = "test-app-id"
        collector.api_url = "http://localhost:4040/api/v1"

        # Simulate timeout
        mock_get.side_effect = requests.exceptions.Timeout("Request timed out")

        # Should return empty list, not crash
        metrics = collector.collect_stage_metrics([0])
        assert metrics == []

    @patch('requests.get')
    @patch('app.spark.stage_metrics_collector_rest.StageMetricsCollectorREST.__init__')
    def test_rest_collector_handles_404(self, mock_init, mock_get):
        """Test REST collector handles 404 responses gracefully"""
        from app.spark.stage_metrics_collector_rest import StageMetricsCollectorREST

        mock_init.return_value = None
        collector = StageMetricsCollectorREST(None)
        collector.app_id = "test-app-id"
        collector.api_url = "http://localhost:4040/api/v1"

        # Simulate 404 response
        response = Mock()
        response.status_code = 404
        mock_get.return_value = response

        # Should return empty list, not crash
        metrics = collector.collect_stage_metrics([0])
        assert metrics == []

    @patch('requests.get')
    @patch('app.spark.stage_metrics_collector_rest.StageMetricsCollectorREST.__init__')
    def test_rest_collector_handles_invalid_json(self, mock_init, mock_get):
        """Test REST collector handles invalid JSON responses"""
        from app.spark.stage_metrics_collector_rest import StageMetricsCollectorREST

        mock_init.return_value = None
        collector = StageMetricsCollectorREST(None)
        collector.app_id = "test-app-id"
        collector.api_url = "http://localhost:4040/api/v1"

        # Simulate successful response with invalid JSON
        response = Mock()
        response.status_code = 200
        response.json.side_effect = ValueError("Invalid JSON")
        mock_get.return_value = response

        # Should return empty list, not crash
        metrics = collector.collect_stage_metrics([0])
        assert metrics == []


class TestFactoryPatternUnit:
    """Unit tests for factory pattern without PySpark dependency"""

    @patch('app.spark.stage_metrics_factory.StageMetricsCollectorREST')
    def test_factory_default_creates_rest_collector(self, mock_rest_class):
        """Test that factory creates REST collector by default"""
        from app.spark.stage_metrics_factory import create_stage_metrics_collector

        mock_spark = Mock()
        mock_rest_instance = Mock()
        mock_rest_class.return_value = mock_rest_instance

        collector = create_stage_metrics_collector(mock_spark)

        # Should have called REST constructor
        mock_rest_class.assert_called_once_with(mock_spark)
        assert collector == mock_rest_instance

    @patch('app.spark.stage_metrics_factory.StageMetricsCollectorREST')
    def test_factory_explicit_rest_type(self, mock_rest_class):
        """Test factory with explicit REST type"""
        from app.spark.stage_metrics_factory import create_stage_metrics_collector

        mock_spark = Mock()
        mock_rest_instance = Mock()
        mock_rest_class.return_value = mock_rest_instance

        collector = create_stage_metrics_collector(mock_spark, collector_type='rest')

        mock_rest_class.assert_called_once_with(mock_spark)
        assert collector == mock_rest_instance

    @patch.dict('os.environ', {'SPARK_METRICS_COLLECTOR_TYPE': 'rest'})
    @patch('app.spark.stage_metrics_factory.StageMetricsCollectorREST')
    def test_factory_env_override_rest(self, mock_rest_class):
        """Test factory respects environment variable override for REST"""
        from app.spark.stage_metrics_factory import create_stage_metrics_collector

        mock_spark = Mock()
        mock_rest_instance = Mock()
        mock_rest_class.return_value = mock_rest_instance

        # Even if we request java, env var should override to rest
        collector = create_stage_metrics_collector(mock_spark, collector_type='java')

        # Should use REST from env var, not Java from parameter
        mock_rest_class.assert_called_once_with(mock_spark)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
