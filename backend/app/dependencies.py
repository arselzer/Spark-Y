"""
Dependency injection for FastAPI routes
"""
from fastapi import HTTPException
from typing import Optional

from app.spark.spark_manager import SparkManager

# Global Spark manager instance
_spark_manager: Optional[SparkManager] = None


def set_spark_manager(manager: SparkManager):
    """Set the global Spark manager instance"""
    global _spark_manager
    _spark_manager = manager


def get_spark_manager() -> SparkManager:
    """Dependency to get Spark manager"""
    if _spark_manager is None:
        raise HTTPException(status_code=503, detail="Spark not initialized")
    return _spark_manager
