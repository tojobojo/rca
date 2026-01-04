from databricks.connect import DatabricksSession
from utils.logging_config import get_logger

logger = get_logger(__name__)

def get_spark_session():
    """Get Spark session - optimized for Databricks serverless"""
    try:
        return DatabricksSession.builder.serverless().getOrCreate()
    except Exception as e:
        logger.error(f"Failed to get Spark session: {e}")
        return None