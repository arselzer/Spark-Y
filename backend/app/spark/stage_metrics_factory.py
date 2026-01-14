"""
Factory for creating stage metrics collectors

Supports two implementations:
1. Java interop (direct access to Spark internal AppStatusStore)
2. REST API (official Spark monitoring API)
"""
import logging
import os
from typing import Dict, Literal
from pyspark.sql import SparkSession

from app.spark.stage_metrics_collector import StageMetricsCollector as JavaCollector
from app.spark.stage_metrics_collector_rest import StageMetricsCollectorREST

logger = logging.getLogger(__name__)

CollectorType = Literal["java", "rest", "auto"]


def create_stage_metrics_collector(
    spark: SparkSession,
    collector_type: CollectorType = "rest"
):
    """
    Create a stage metrics collector

    Args:
        spark: SparkSession instance
        collector_type: Type of collector to create:
            - "java": Use Java interop (fastest, internal API)
            - "rest": Use REST API (stable, official API)
            - "auto": Try Java first, fall back to REST (default)

    Returns:
        Stage metrics collector instance
    """
    # Check environment variable override
    env_type = os.getenv("SPARK_METRICS_COLLECTOR_TYPE", "").lower()
    if env_type in ["java", "rest"]:
        collector_type = env_type
        logger.info(f"Using metrics collector type from env: {collector_type}")

    if collector_type == "java":
        logger.info("Creating Java interop stage metrics collector")
        return JavaCollector(spark)

    elif collector_type == "rest":
        logger.info("Creating REST API stage metrics collector")
        return StageMetricsCollectorREST(spark)

    elif collector_type == "auto":
        # Try Java first (faster), fall back to REST if it fails
        try:
            collector = JavaCollector(spark)
            # Test if Java interop works by checking if we can access status tracker
            _ = collector.status_tracker
            logger.info("Using Java interop stage metrics collector (auto-selected)")
            return collector
        except Exception as e:
            logger.warning(f"Java interop collector failed, falling back to REST API: {e}")
            return StageMetricsCollectorREST(spark)

    else:
        logger.warning(f"Unknown collector type '{collector_type}', using auto")
        return create_stage_metrics_collector(spark, "auto")


def get_collector_info(collector) -> Dict[str, str]:
    """
    Get information about which collector implementation is being used

    Args:
        collector: Stage metrics collector instance

    Returns:
        Dictionary with collector type and description
    """
    if isinstance(collector, JavaCollector):
        return {
            "type": "java",
            "description": "Java interop (direct AppStatusStore access)",
            "performance": "fastest",
            "stability": "internal API"
        }
    elif isinstance(collector, StageMetricsCollectorREST):
        return {
            "type": "rest",
            "description": "REST API (Spark monitoring API)",
            "performance": "moderate (HTTP overhead)",
            "stability": "official API"
        }
    else:
        return {
            "type": "unknown",
            "description": "Unknown collector type",
            "performance": "unknown",
            "stability": "unknown"
        }
