from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from config.settings import Config
from utils.logging_config import setup_logging

logger = setup_logging("baseline_enricher")


class BaselineEnricher:
    """Calculate and enrich metrics with baseline comparisons"""

    def __init__(self):
        self.short_window = Config.rca.BASELINE_DAYS_SHORT
        self.long_window = Config.rca.BASELINE_DAYS_LONG

    def enrich_with_baselines(
        self,
        current_metrics_df: DataFrame,
        pipeline_name: str
    ) -> DataFrame:
        """
        Enrich current metrics with baseline calculations

        Args:
            current_metrics_df: Current run metrics
            pipeline_name: Pipeline name

        Returns:
            Enriched DataFrame with baseline columns
        """
        logger.info(f"Enriching metrics with baselines for {pipeline_name}")

        try:
            # Get historical metrics for baseline calculation
            historical_df = self._get_historical_metrics(pipeline_name, current_metrics_df)

            # Use cheap emptiness check to avoid full dataframe count
            if historical_df.limit(1).count() == 0:
                logger.warning(f"No historical data for {pipeline_name}, skipping baselines")
                return self._add_null_baselines(current_metrics_df)

            # Calculate baselines per rule
            baselines_df = self._calculate_baselines(historical_df)

            # Join with current metrics
            enriched_df = current_metrics_df.join(
                baselines_df,
                on=["pipeline_name", "step_name", "rule_name"],
                how="left"
            )

            # Fill nulls with 0 for new rules
            enriched_df = enriched_df.fillna(0, subset=[
                "drop_percentage_7day_avg",
                "drop_percentage_30day_avg"
            ])

            logger.info("Baseline enrichment completed")
            return enriched_df

        except Exception as e:
            logger.error(f"Baseline enrichment failed: {e}", exc_info=True)
            return self._add_null_baselines(current_metrics_df)

    def _get_historical_metrics(self, pipeline_name: str, current_metrics_df: DataFrame) -> DataFrame:
        """Get historical metrics for baseline calculation"""
        from core.spark_manager import SparkManager
        spark = SparkManager.get_session()

        # Check if table exists
        if not SparkManager.table_exists(Config.db.METRICS_TABLE):
            logger.warning(f"Metrics table doesn't exist yet. Using current run data only")
            return current_metrics_df.select(
                "pipeline_name", "step_name", "rule_name", "run_timestamp", "drop_percentage"
            )

        # Build DataFrame filter using DataFrame API to avoid string interpolation
        cutoff_expr = F.expr(f"CAST(DATE_SUB(CURRENT_DATE(), {self.long_window}) AS TIMESTAMP)")

        df = (
            spark.table(Config.db.METRICS_TABLE)
            .select("pipeline_name", "step_name", "rule_name", "run_timestamp", "drop_percentage")
            .filter((F.col("pipeline_name") == pipeline_name)
                    & (F.col("run_timestamp") >= cutoff_expr)
                    & (F.col("run_timestamp") < F.current_timestamp()))
        )

        return df

    def _calculate_baselines(self, historical_df: DataFrame) -> DataFrame:
        """Calculate 7-day and 30-day rolling averages"""
        # Use SQL expressions for date windows inside aggregations to avoid
        # constructing timestamp objects in the DataFrame API which can be
        # error-prone across Spark versions.
        # Use expressions that are compatible with Spark SQL for windowed averages
        short_cutoff = f"CAST(DATE_SUB(CURRENT_DATE(), {self.short_window}) AS TIMESTAMP)"
        long_cutoff = f"CAST(DATE_SUB(CURRENT_DATE(), {self.long_window}) AS TIMESTAMP)"

        baselines_df = historical_df.groupBy(
            "pipeline_name", "step_name", "rule_name"
        ).agg(
            F.expr(f"avg(case when run_timestamp >= {short_cutoff} then drop_percentage end)").alias("drop_percentage_7day_avg"),
            F.expr(f"avg(case when run_timestamp >= {long_cutoff} then drop_percentage end)").alias("drop_percentage_30day_avg"),
            F.stddev(F.col("drop_percentage")).alias("drop_percentage_stddev")
        )

        return baselines_df

    def _add_null_baselines(self, df: DataFrame) -> DataFrame:
        """Add null baseline columns"""
        return df.withColumn("drop_percentage_7day_avg", F.lit(None).cast("double")) \
                 .withColumn("drop_percentage_30day_avg", F.lit(None).cast("double")) \
                 .withColumn("drop_percentage_stddev", F.lit(None).cast("double"))

