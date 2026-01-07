from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Dict, Any, Optional
from datetime import datetime
from config.settings import Config
from core.spark_manager import SparkManager
from core.exceptions import MetricsCalculationError, DataValidationError
from metrics.baseline_enricher import BaselineEnricher
from utils.logging_config import setup_logging

logger = setup_logging("metrics_calculator")


class MetricsCalculator:
    """
    Calculate pipeline metrics from audit table and enrich with baselines.
    This is the single entry point for all metrics-related operations.
    """
    
    def __init__(self):
        self.baseline_enricher = BaselineEnricher()
        self.spark = SparkManager.get_session()
        self.audit_table = Config.db.AUDIT_TABLE
        self.metrics_table = Config.db.METRICS_TABLE
    
    def process_pipeline_run(
        self,
        run_id: str,
        pipeline_name: str
    ) -> Dict[str, Any]:
        """
        Complete metrics processing for a pipeline run.
        This is the ONLY method you need to call.
        
        Args:
            run_id: Unique run identifier
            pipeline_name: Pipeline name
            
        Returns:
            Dict with processing results and metrics DataFrame
        """
        logger.info("="*80)
        logger.info(f"Processing metrics for {pipeline_name}, run_id={run_id}")
        logger.info("="*80)
        
        try:
            # Step 1: Extract raw metrics from audit table
            logger.info("Step 1: Extracting metrics from audit table...")
            raw_metrics_df = self._extract_from_audit_table(run_id, pipeline_name)
            
            if raw_metrics_df.count() == 0:
                raise DataValidationError(
                    f"No audit data found for run_id={run_id}, pipeline={pipeline_name}"
                )
            
            logger.info(f"✓ Extracted {raw_metrics_df.count()} raw metric records")
            
            # Step 2: Calculate drop percentages
            logger.info("Step 2: Calculating drop percentages...")
            metrics_df = self._calculate_drop_percentages(raw_metrics_df)
            
            # Step 3: Check for code changes
            logger.info("Step 3: Checking for code changes...")
            metrics_df = self._enrich_with_code_change_info(metrics_df, pipeline_name)
            
            # Step 4: Enrich with baselines (7-day, 30-day averages)
            logger.info("Step 4: Enriching with baseline comparisons...")
            metrics_df = self.baseline_enricher.enrich_with_baselines(
                metrics_df, pipeline_name
            )
            
            # Step 5: Store in Delta table
            logger.info("Step 5: Storing metrics to Delta table...")
            self._store_metrics(metrics_df)
            
            logger.info("="*80)
            logger.info(f"✓ Metrics processing completed successfully")
            logger.info(f"Records processed: {metrics_df.count()}")
            logger.info("="*80)
            
            return {
                'success': True,
                'run_id': run_id,
                'pipeline_name': pipeline_name,
                'records_processed': metrics_df.count(),
                'metrics_df': metrics_df
            }
            
        except Exception as e:
            logger.error("="*80)
            logger.error(f"✗ Metrics processing FAILED")
            logger.error(f"Error: {str(e)}")
            logger.error("="*80)
            logger.exception("Full traceback:")
            raise MetricsCalculationError(f"Failed to process metrics: {e}")
    
    def _extract_from_audit_table(
        self, 
        run_id: str, 
        pipeline_name: str
    ) -> DataFrame:
        """
        Extract metrics from audit table.
        
        Expected audit table schema:
        - run_id STRING
        - pipeline_name STRING
        - step_name STRING
        - step_order INT
        - rule_name STRING
        - rule_version STRING (or rule_version_id)
        - rule_definition_hash STRING (optional)
        - input_records BIGINT
        - output_records BIGINT
        - run_timestamp TIMESTAMP
        """
        logger.debug(f"Querying audit table: {self.audit_table}")
        
        query = f"""
        SELECT 
            run_id,
            pipeline_name,
            run_timestamp,
            step_name,
            COALESCE(step_order, 0) as step_order,
            rule_name,
            rule_version as rule_version_id,
            rule_definition_hash,
            input_records,
            output_records,
            (input_records - output_records) as dropped_records
        FROM {self.audit_table}
        WHERE run_id = '{run_id}'
          AND pipeline_name = '{pipeline_name}'
        ORDER BY step_order, rule_name
        """
        
        try:
            df = SparkManager.execute_query(query)
            return df
        except Exception as e:
            logger.error(f"Failed to query audit table: {e}")
            raise DataValidationError(f"Audit table query failed: {e}")
    
    def _calculate_drop_percentages(self, df: DataFrame) -> DataFrame:
        """Calculate drop percentages for each rule"""
        logger.debug("Calculating drop percentages")
        
        return df.withColumn(
            "drop_percentage",
            F.when(
                F.col("input_records") > 0,
                (F.col("dropped_records") / F.col("input_records") * 100)
            ).otherwise(0.0)
        )
    
    def _enrich_with_code_change_info(
        self, 
        df: DataFrame, 
        pipeline_name: str
    ) -> DataFrame:
        """
        Enrich with code change information.
        Checks if rules were recently changed.
        """
        logger.debug("Enriching with code change information")
        
        try:
            change_window_hours = Config.rca.CODE_CHANGE_WINDOW_HOURS
            
            # Query recent code changes
            query = f"""
            SELECT 
                old_rule_version_id as rule_version_id,
                change_id as recent_change_id,
                change_timestamp,
                DATEDIFF(HOUR, change_timestamp, CURRENT_TIMESTAMP) as hours_since_change
            FROM {Config.db.RULE_CHANGES_TABLE}
            WHERE pipeline_name = '{pipeline_name}'
              -- AND change_timestamp >= CURRENT_TIMESTAMP - INTERVAL {change_window_hours} HOURS
            """
            
            changes_df = SparkManager.execute_query(query)
            
            if changes_df.count() == 0:
                # No recent changes
                logger.debug("No recent code changes found")
                return df.withColumn("is_first_run_after_change", F.lit(False)) \
                        .withColumn("days_since_rule_changed", F.lit(None).cast("int")) \
                        .withColumn("recent_change_id", F.lit(None).cast("string"))
            
            # Join with changes
            enriched_df = df.join(
                changes_df,
                on="rule_version_id",
                how="left"
            )
            
            # Mark first run after change
            enriched_df = enriched_df.withColumn(
                "is_first_run_after_change",
                F.col("recent_change_id").isNotNull()
            ).withColumn(
                "days_since_rule_changed",
                F.when(
                    F.col("hours_since_change").isNotNull(),
                    (F.col("hours_since_change") / 24).cast("int")
                ).otherwise(None)
            )
            
            # Clean up temporary columns
            enriched_df = enriched_df.drop("hours_since_change")
            
            logger.debug("Code change enrichment completed")
            return enriched_df
            
        except Exception as e:
            logger.warning(f"Could not enrich with code changes: {e}")
            # Return with null columns if enrichment fails
            return df.withColumn("is_first_run_after_change", F.lit(False)) \
                    .withColumn("days_since_rule_changed", F.lit(None).cast("int")) \
                    .withColumn("recent_change_id", F.lit(None).cast("string"))
    
    def _store_metrics(self, df: DataFrame):
        """Store enriched metrics in Delta table"""
        logger.debug(f"Storing metrics to {self.metrics_table}")
        
        try:
            from delta.tables import DeltaTable
            # Check if table exists
            if SparkManager.table_exists(self.metrics_table):
                # Table exists - use MERGE (upsert)
                delta_table = DeltaTable.forName(self.spark, self.metrics_table)
                
                delta_table.alias("target").merge(
                    df.alias("source"),
                    "target.run_id = source.run_id AND target.pipeline_name = source.pipeline_name AND target.step_name = source.step_name AND target.rule_name = source.rule_name AND target.recent_change_id = source.recent_change_id"
                ).whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
                
                logger.info(f"✓ Metrics upserted successfully")
            else:
                df.write.format("delta") \
                    .mode("append") \
                    .partitionBy("pipeline_name") \
                    .saveAsTable(self.metrics_table)
                
                logger.info(f"✓ Metrics stored successfully")
            
        except Exception as e:
            logger.error(f"Failed to store metrics: {e}")
            raise MetricsCalculationError(f"Failed to store metrics: {e}")
