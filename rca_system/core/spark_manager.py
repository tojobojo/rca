from pyspark.sql import SparkSession, DataFrame
from typing import Optional
from utils.logging_config import setup_logging

logger = setup_logging("spark_manager")


class SparkManager:
	"""Manage Spark session and operations"""
    
	_instance: Optional[SparkSession] = None
    
	@classmethod
	def get_session(cls) -> SparkSession:
		"""Get or create Spark session"""
		if cls._instance is None:
			try:
				cls._instance = SparkSession.builder.getOrCreate()
				logger.info("Spark session created successfully")
			except Exception as e:
				logger.error(f"Failed to create Spark session: {e}")
				raise
        
		return cls._instance
    
	@classmethod
	def execute_query(cls, query: str) -> DataFrame:
		"""Execute SQL query and return DataFrame"""
		try:
			spark = cls.get_session()
			logger.debug(f"Executing query: {query[:200]}...")
			df = spark.sql(query)
			return df
		except Exception as e:
			logger.error(f"Query execution failed: {e}")
			raise
    
	@classmethod
	def table_exists(cls, table_name: str) -> bool:
		"""Check if table exists"""
		try:
			spark = cls.get_session()
			# Use catalog API to avoid constructing SQL strings
			return spark.catalog.tableExists(table_name)
		except Exception:
			return False
