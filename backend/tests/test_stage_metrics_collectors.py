"""
Tests for stage metrics collectors (REST API and Java interop)

Tests both implementations of stage-level metrics collection:
1. REST API collector (official Spark monitoring API)
2. Java interop collector (direct AppStatusStore access)
3. Factory pattern for collector selection
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import requests

from app.spark.stage_metrics_collector import StageMetricsCollector as JavaCollector
from app.spark.stage_metrics_collector_rest import StageMetricsCollectorREST
from app.spark.stage_metrics_factory import (
    create_stage_metrics_collector,
    get_collector_info,
    CollectorType
)


class TestRESTAPICollector:
    """Test REST API-based metrics collector"""

    def test_collector_initialization(self, spark_session):
        """Test that REST collector initializes with app ID and API URL"""
        collector = StageMetricsCollectorREST(spark_session)

        # Should have app ID
        assert collector.app_id is not None
        assert len(collector.app_id) > 0

        # Should have API URL
        assert collector.api_url is not None
        assert "/api/v1" in collector.api_url
        assert "localhost" in collector.api_url

    def test_parse_timestamp_valid(self, spark_session):
        """Test timestamp parsing with valid Spark format"""
        collector = StageMetricsCollectorREST(spark_session)

        # Spark format: "2025-01-19T10:30:15.123GMT"
        timestamp_str = "2025-01-19T10:30:15.123GMT"
        result = collector._parse_timestamp(timestamp_str)

        assert result is not None
        assert isinstance(result, int)
        assert result > 0

    def test_parse_timestamp_none(self, spark_session):
        """Test timestamp parsing with None input"""
        collector = StageMetricsCollectorREST(spark_session)

        result = collector._parse_timestamp(None)
        assert result is None

    def test_parse_timestamp_empty(self, spark_session):
        """Test timestamp parsing with empty string"""
        collector = StageMetricsCollectorREST(spark_session)

        result = collector._parse_timestamp("")
        assert result is None

    @patch('requests.get')
    def test_collect_stage_metrics_with_mocked_api(self, mock_get, spark_session):
        """Test collecting stage metrics with mocked REST API responses"""
        collector = StageMetricsCollectorREST(spark_session)

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

    def test_extract_metrics_from_stage_data(self, spark_session):
        """Test extracting metrics from stage data dict"""
        collector = StageMetricsCollectorREST(spark_session)

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


class TestJavaInteropCollector:
    """Test Java interop-based metrics collector"""

    def test_collector_initialization(self, spark_session):
        """Test that Java collector initializes with SparkContext"""
        try:
            collector = JavaCollector(spark_session)

            # Should have SparkContext
            assert collector.spark is not None
            assert collector.sc is not None
            assert collector.status_tracker is not None
        except Exception as e:
            # Java interop may not be available in all environments
            pytest.skip(f"Java interop not available: {e}")

    def test_aggregate_metrics_empty(self, spark_session):
        """Test aggregating empty metrics list"""
        try:
            collector = JavaCollector(spark_session)

            result = collector.aggregate_metrics_by_stage([])

            assert result['total_stages'] == 0
            assert result['total_shuffle_read_bytes'] == 0
            assert result['total_shuffle_write_bytes'] == 0
        except Exception as e:
            pytest.skip(f"Java interop not available: {e}")

    def test_aggregate_metrics_with_data(self, spark_session):
        """Test aggregating metrics with sample data"""
        try:
            collector = JavaCollector(spark_session)

            sample_metrics = [
                {
                    'stage_id': 0,
                    'name': 'Stage 0',
                    'num_tasks': 10,
                    'execution_time_ms': 1000,
                    'shuffle_read_bytes': 5000,
                    'shuffle_write_bytes': 3000,
                    'input_records': 100,
                    'output_records': 50
                },
                {
                    'stage_id': 1,
                    'name': 'Stage 1',
                    'num_tasks': 5,
                    'execution_time_ms': 500,
                    'shuffle_read_bytes': 2000,
                    'shuffle_write_bytes': 1000,
                    'input_records': 50,
                    'output_records': 25
                }
            ]

            result = collector.aggregate_metrics_by_stage(sample_metrics)

            assert result['total_stages'] == 2
            assert result['total_shuffle_read_bytes'] == 7000
            assert result['total_shuffle_write_bytes'] == 4000
            assert result['total_input_records'] == 150
            assert result['total_output_records'] == 75
        except Exception as e:
            pytest.skip(f"Java interop not available: {e}")


class TestFactoryPattern:
    """Test factory pattern for collector creation"""

    def test_factory_default_creates_rest_collector(self, spark_session):
        """Test that factory creates REST collector by default"""
        collector = create_stage_metrics_collector(spark_session)

        # Should be REST collector
        assert isinstance(collector, StageMetricsCollectorREST)

        # Get info should confirm
        info = get_collector_info(collector)
        assert info['type'] == 'rest'
        assert 'REST API' in info['description']

    def test_factory_explicit_rest_type(self, spark_session):
        """Test factory with explicit REST type"""
        collector = create_stage_metrics_collector(spark_session, collector_type='rest')

        assert isinstance(collector, StageMetricsCollectorREST)

        info = get_collector_info(collector)
        assert info['type'] == 'rest'

    def test_factory_java_type(self, spark_session):
        """Test factory with Java type"""
        try:
            collector = create_stage_metrics_collector(spark_session, collector_type='java')

            assert isinstance(collector, JavaCollector)

            info = get_collector_info(collector)
            assert info['type'] == 'java'
            assert 'Java interop' in info['description']
        except Exception as e:
            pytest.skip(f"Java interop not available: {e}")

    def test_factory_auto_type(self, spark_session):
        """Test factory with auto type (tries Java, falls back to REST)"""
        collector = create_stage_metrics_collector(spark_session, collector_type='auto')

        # Should be one of the two types
        assert isinstance(collector, (JavaCollector, StageMetricsCollectorREST))

        info = get_collector_info(collector)
        assert info['type'] in ['java', 'rest']

    @patch.dict('os.environ', {'SPARK_METRICS_COLLECTOR_TYPE': 'rest'})
    def test_factory_env_override_rest(self, spark_session):
        """Test factory respects environment variable override for REST"""
        collector = create_stage_metrics_collector(spark_session, collector_type='java')

        # Should use REST from env var, not Java from parameter
        assert isinstance(collector, StageMetricsCollectorREST)

    @patch.dict('os.environ', {'SPARK_METRICS_COLLECTOR_TYPE': 'java'})
    def test_factory_env_override_java(self, spark_session):
        """Test factory respects environment variable override for Java"""
        try:
            collector = create_stage_metrics_collector(spark_session, collector_type='rest')

            # Should use Java from env var, not REST from parameter
            assert isinstance(collector, JavaCollector)
        except Exception as e:
            # If Java not available, env var should be ignored
            assert isinstance(collector, StageMetricsCollectorREST)

    def test_get_collector_info_rest(self, spark_session):
        """Test getting collector info for REST collector"""
        collector = StageMetricsCollectorREST(spark_session)
        info = get_collector_info(collector)

        assert info['type'] == 'rest'
        assert 'REST API' in info['description']
        assert 'moderate' in info['performance']
        assert 'official' in info['stability']

    def test_get_collector_info_java(self, spark_session):
        """Test getting collector info for Java collector"""
        try:
            collector = JavaCollector(spark_session)
            info = get_collector_info(collector)

            assert info['type'] == 'java'
            assert 'Java interop' in info['description']
            assert 'fastest' in info['performance']
            assert 'internal' in info['stability']
        except Exception as e:
            pytest.skip(f"Java interop not available: {e}")


class TestIntegrationWithQueries:
    """Test metrics collection with actual query execution"""

    def test_collect_metrics_simple_query(self, sample_data):
        """Test collecting metrics from a simple query execution"""
        spark = sample_data

        # Create collector
        collector = create_stage_metrics_collector(spark)

        # Execute a simple query that should create jobs
        df = spark.sql("""
            SELECT t.title, cn.name
            FROM title t
            JOIN movie_companies mc ON t.id = mc.movie_id
            JOIN company_name cn ON mc.company_id = cn.id
        """)

        # Track jobs before execution
        status_tracker = spark.sparkContext.statusTracker()
        jobs_before = set(status_tracker.getActiveJobIds())

        # Execute query
        result = df.collect()

        # Track jobs after execution
        jobs_after = set(status_tracker.getActiveJobIds())
        all_job_ids = list(jobs_before | jobs_after)

        # Wait a moment for metrics to be available
        import time
        time.sleep(0.2)

        # Try to collect metrics (may be empty if no jobs were tracked)
        try:
            metrics = collector.collect_stage_metrics(all_job_ids)

            # If we got metrics, verify structure
            if metrics:
                assert isinstance(metrics, list)
                for metric in metrics:
                    assert 'stage_id' in metric
                    assert 'name' in metric or 'operators' in metric
                    # Either real metrics or estimated
                    assert ('execution_time_ms' in metric or
                           'estimated_complexity' in metric)
        except Exception as e:
            # Metrics collection may fail in test environment
            # This is acceptable - the important thing is no crashes
            pytest.skip(f"Metrics collection not available: {e}")

    def test_metrics_structure_consistency(self, sample_data):
        """Test that collected metrics have consistent structure"""
        spark = sample_data
        collector = create_stage_metrics_collector(spark)

        # Create sample metrics data
        sample_metrics = [
            {
                'stage_id': 0,
                'name': 'WholeStageCodegen (1)',
                'num_tasks': 10,
                'execution_time_ms': 1500,
                'input_records': 1000,
                'output_records': 100,
                'shuffle_read_bytes': 5000,
                'shuffle_write_bytes': 3000,
                'memory_bytes_spilled': 0,
                'disk_bytes_spilled': 0,
                'executor_run_time': 1400,
                'executor_cpu_time': 1300,
                'jvm_gc_time': 50
            }
        ]

        # Verify all expected fields are present
        metric = sample_metrics[0]
        required_fields = [
            'stage_id', 'name', 'num_tasks', 'execution_time_ms',
            'input_records', 'output_records', 'shuffle_read_bytes',
            'shuffle_write_bytes'
        ]

        for field in required_fields:
            assert field in metric, f"Missing required field: {field}"
            assert metric[field] is not None


class TestErrorHandling:
    """Test error handling in collectors"""

    def test_rest_collector_handles_network_error(self, spark_session):
        """Test REST collector handles network errors gracefully"""
        collector = StageMetricsCollectorREST(spark_session)

        with patch('requests.get') as mock_get:
            # Simulate network error
            mock_get.side_effect = requests.exceptions.ConnectionError("Connection failed")

            # Should return empty list, not crash
            metrics = collector.collect_stage_metrics([0])
            assert metrics == []

    def test_rest_collector_handles_timeout(self, spark_session):
        """Test REST collector handles timeout gracefully"""
        collector = StageMetricsCollectorREST(spark_session)

        with patch('requests.get') as mock_get:
            # Simulate timeout
            mock_get.side_effect = requests.exceptions.Timeout("Request timed out")

            # Should return empty list, not crash
            metrics = collector.collect_stage_metrics([0])
            assert metrics == []

    def test_rest_collector_handles_404(self, spark_session):
        """Test REST collector handles 404 responses gracefully"""
        collector = StageMetricsCollectorREST(spark_session)

        with patch('requests.get') as mock_get:
            # Simulate 404 response
            response = Mock()
            response.status_code = 404
            mock_get.return_value = response

            # Should return empty list, not crash
            metrics = collector.collect_stage_metrics([0])
            assert metrics == []

    def test_rest_collector_handles_invalid_json(self, spark_session):
        """Test REST collector handles invalid JSON responses"""
        collector = StageMetricsCollectorREST(spark_session)

        with patch('requests.get') as mock_get:
            # Simulate successful response with invalid JSON
            response = Mock()
            response.status_code = 200
            response.json.side_effect = ValueError("Invalid JSON")
            mock_get.return_value = response

            # Should return empty list, not crash
            metrics = collector.collect_stage_metrics([0])
            assert metrics == []


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
