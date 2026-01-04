# agent_manager.py - Agent configuration for Pipeline RCA Analysis
from agents import Agent, function_tool, ModelSettings
from agents.guardrail import InputGuardrail, OutputGuardrail, GuardrailFunctionOutput
from typing import Dict, Any, List, Optional
from config import Config
import logging
import re
from datetime import datetime, timedelta
import json

logger = logging.getLogger("rca_bot.agents")

# ============================================================================
# DATABRICKS CONNECTION HELPER
# ============================================================================

def get_spark_session():
    """Get or create Spark session for Databricks"""
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        return spark
    except Exception as e:
        logger.error(f"Failed to get Spark session: {e}")
        return None


# ============================================================================
# FUNCTION TOOLS - Pipeline RCA Analysis Tools
# ============================================================================

@function_tool
def query_pipeline_metrics(
    pipeline_name: str = None,
    start_date: str = None,
    end_date: str = None,
    step_name: str = None,
    rule_name: str = None,
    limit: int = 100
) -> str:
    """
    Query pipeline run metrics from Delta tables.
    
    Args:
        pipeline_name: Name of the pipeline to analyze (optional)
        start_date: Start date in YYYY-MM-DD format (optional)
        end_date: End date in YYYY-MM-DD format (optional)
        step_name: Specific step to filter by (optional)
        rule_name: Specific rule to filter by (optional)
        limit: Maximum number of records to return (default: 100)
    
    Returns:
        JSON formatted metrics data with run_id, timestamps, drop counts, and percentages
    """
    logger.info(f"Querying pipeline metrics: pipeline={pipeline_name}, dates={start_date} to {end_date}")
    
    spark = get_spark_session()
    if not spark:
        return json.dumps({"error": "Unable to connect to Spark session"})
    
    try:
        # Build query dynamically
        query = "SELECT run_id, pipeline_name, run_timestamp, step_name, rule_name, "
        query += "input_records, output_records, dropped_records, drop_percentage, "
        query += "rule_version_id, is_first_run_after_change, days_since_rule_changed, "
        query += "drop_percentage_7day_avg, drop_percentage_30day_avg "
        query += "FROM pipeline_run_metrics WHERE 1=1"
        
        if pipeline_name:
            query += f" AND pipeline_name = '{pipeline_name}'"
        
        if start_date:
            query += f" AND DATE(run_timestamp) >= '{start_date}'"
        
        if end_date:
            query += f" AND DATE(run_timestamp) <= '{end_date}'"
        
        if step_name:
            query += f" AND step_name = '{step_name}'"
        
        if rule_name:
            query += f" AND rule_name = '{rule_name}'"
        
        query += f" ORDER BY run_timestamp DESC LIMIT {limit}"
        
        logger.debug(f"Executing query: {query}")
        
        df = spark.sql(query)
        results = df.collect()
        
        if not results:
            return json.dumps({
                "message": "No metrics found matching the criteria",
                "filters": {
                    "pipeline": pipeline_name,
                    "start_date": start_date,
                    "end_date": end_date,
                    "step": step_name,
                    "rule": rule_name
                }
            })
        
        # Convert to list of dicts
        metrics_list = []
        for row in results:
            metrics_list.append({
                "run_id": row.run_id,
                "pipeline_name": row.pipeline_name,
                "run_timestamp": str(row.run_timestamp),
                "step_name": row.step_name,
                "rule_name": row.rule_name,
                "input_records": int(row.input_records),
                "output_records": int(row.output_records),
                "dropped_records": int(row.dropped_records),
                "drop_percentage": float(row.drop_percentage),
                "rule_version_id": row.rule_version_id,
                "is_first_run_after_change": row.is_first_run_after_change,
                "days_since_rule_changed": row.days_since_rule_changed,
                "baseline_7day_avg": float(row.drop_percentage_7day_avg) if row.drop_percentage_7day_avg else None,
                "baseline_30day_avg": float(row.drop_percentage_30day_avg) if row.drop_percentage_30day_avg else None
            })
        
        return json.dumps({
            "count": len(metrics_list),
            "metrics": metrics_list
        }, indent=2)
        
    except Exception as e:
        logger.error(f"Error querying metrics: {e}", exc_info=True)
        return json.dumps({"error": f"Query failed: {str(e)}"})


@function_tool
def compare_pipeline_runs(run_id_1: str, run_id_2: str) -> str:
    """
    Compare metrics between two specific pipeline runs.
    
    Args:
        run_id_1: First run ID to compare
        run_id_2: Second run ID to compare
    
    Returns:
        Detailed comparison showing differences in drop rates across steps and rules
    """
    logger.info(f"Comparing runs: {run_id_1} vs {run_id_2}")
    
    spark = get_spark_session()
    if not spark:
        return json.dumps({"error": "Unable to connect to Spark session"})
    
    try:
        query = f"""
        SELECT 
            run_id,
            step_name,
            rule_name,
            input_records,
            output_records,
            dropped_records,
            drop_percentage,
            run_timestamp
        FROM pipeline_run_metrics
        WHERE run_id IN ('{run_id_1}', '{run_id_2}')
        ORDER BY run_id, step_name, rule_name
        """
        
        df = spark.sql(query)
        results = df.collect()
        
        if not results:
            return json.dumps({"error": "No data found for the specified run IDs"})
        
        # Organize by run
        run1_data = {}
        run2_data = {}
        
        for row in results:
            key = f"{row.step_name}:{row.rule_name}"
            data = {
                "step": row.step_name,
                "rule": row.rule_name,
                "input": int(row.input_records),
                "output": int(row.output_records),
                "dropped": int(row.dropped_records),
                "drop_pct": float(row.drop_percentage),
                "timestamp": str(row.run_timestamp)
            }
            
            if row.run_id == run_id_1:
                run1_data[key] = data
            else:
                run2_data[key] = data
        
        # Calculate differences
        comparison = []
        all_keys = set(run1_data.keys()) | set(run2_data.keys())
        
        for key in all_keys:
            r1 = run1_data.get(key)
            r2 = run2_data.get(key)
            
            if r1 and r2:
                diff = {
                    "step": r1["step"],
                    "rule": r1["rule"],
                    "run1_drop_pct": r1["drop_pct"],
                    "run2_drop_pct": r2["drop_pct"],
                    "drop_pct_change": round(r2["drop_pct"] - r1["drop_pct"], 2),
                    "dropped_records_change": r2["dropped"] - r1["dropped"],
                    "status": "increased" if r2["drop_pct"] > r1["drop_pct"] else "decreased" if r2["drop_pct"] < r1["drop_pct"] else "unchanged"
                }
            elif r1:
                diff = {
                    "step": r1["step"],
                    "rule": r1["rule"],
                    "run1_drop_pct": r1["drop_pct"],
                    "run2_drop_pct": None,
                    "status": "rule_removed_in_run2"
                }
            else:
                diff = {
                    "step": r2["step"],
                    "rule": r2["rule"],
                    "run1_drop_pct": None,
                    "run2_drop_pct": r2["drop_pct"],
                    "status": "rule_added_in_run2"
                }
            
            comparison.append(diff)
        
        return json.dumps({
            "run_1": run_id_1,
            "run_2": run_id_2,
            "comparison": comparison
        }, indent=2)
        
    except Exception as e:
        logger.error(f"Error comparing runs: {e}", exc_info=True)
        return json.dumps({"error": f"Comparison failed: {str(e)}"})


@function_tool
def get_rule_change_history(
    pipeline_name: str = None,
    rule_name: str = None,
    days_back: int = 30
) -> str:
    """
    Get history of code changes for rules in a pipeline.
    
    Args:
        pipeline_name: Pipeline to check (optional, returns all if not specified)
        rule_name: Specific rule to check (optional)
        days_back: How many days of history to retrieve (default: 30)
    
    Returns:
        List of code changes with timestamps, change types, and diffs
    """
    logger.info(f"Getting rule change history: pipeline={pipeline_name}, rule={rule_name}")
    
    spark = get_spark_session()
    if not spark:
        return json.dumps({"error": "Unable to connect to Spark session"})
    
    try:
        cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        query = f"""
        SELECT 
            change_id,
            change_timestamp,
            pipeline_name,
            step_name,
            rule_name,
            change_type,
            old_rule_definition,
            new_rule_definition,
            diff_summary,
            commit_hash,
            author
        FROM pipeline_rule_changes
        WHERE DATE(change_timestamp) >= '{cutoff_date}'
        """
        
        if pipeline_name:
            query += f" AND pipeline_name = '{pipeline_name}'"
        
        if rule_name:
            query += f" AND rule_name = '{rule_name}'"
        
        query += " ORDER BY change_timestamp DESC"
        
        df = spark.sql(query)
        results = df.collect()
        
        if not results:
            return json.dumps({
                "message": f"No code changes found in the last {days_back} days",
                "filters": {"pipeline": pipeline_name, "rule": rule_name}
            })
        
        changes = []
        for row in results:
            changes.append({
                "change_id": row.change_id,
                "timestamp": str(row.change_timestamp),
                "pipeline": row.pipeline_name,
                "step": row.step_name,
                "rule": row.rule_name,
                "change_type": row.change_type,
                "diff_summary": row.diff_summary,
                "commit_hash": row.commit_hash,
                "author": row.author,
                "old_definition": row.old_rule_definition if row.change_type == "modified_rule" else None,
                "new_definition": row.new_rule_definition if row.change_type != "deleted_rule" else None
            })
        
        return json.dumps({
            "count": len(changes),
            "changes": changes
        }, indent=2)
        
    except Exception as e:
        logger.error(f"Error getting change history: {e}", exc_info=True)
        return json.dumps({"error": f"Query failed: {str(e)}"})


@function_tool
def get_rule_definition(rule_name: str, pipeline_name: str = None, version: str = "current") -> str:
    """
    Get the definition of a specific rule.
    
    Args:
        rule_name: Name of the rule
        pipeline_name: Pipeline name (optional)
        version: Either "current" for latest version or a specific rule_version_id
    
    Returns:
        Rule definition with metadata
    """
    logger.info(f"Getting rule definition: {rule_name}, version={version}")
    
    spark = get_spark_session()
    if not spark:
        return json.dumps({"error": "Unable to connect to Spark session"})
    
    try:
        if version == "current":
            query = f"""
            SELECT 
                rule_id,
                rule_version_id,
                pipeline_name,
                step_name,
                rule_name,
                rule_type,
                rule_definition,
                version_number,
                valid_from,
                valid_to,
                change_type,
                commit_hash
            FROM pipeline_rule_definitions
            WHERE rule_name = '{rule_name}'
              AND is_current = true
            """
            if pipeline_name:
                query += f" AND pipeline_name = '{pipeline_name}'"
        else:
            query = f"""
            SELECT 
                rule_id,
                rule_version_id,
                pipeline_name,
                step_name,
                rule_name,
                rule_type,
                rule_definition,
                version_number,
                valid_from,
                valid_to,
                change_type,
                commit_hash
            FROM pipeline_rule_definitions
            WHERE rule_version_id = '{version}'
            """
        
        df = spark.sql(query)
        results = df.collect()
        
        if not results:
            return json.dumps({"error": f"Rule '{rule_name}' not found"})
        
        row = results[0]
        rule_info = {
            "rule_id": row.rule_id,
            "rule_version_id": row.rule_version_id,
            "pipeline": row.pipeline_name,
            "step": row.step_name,
            "rule_name": row.rule_name,
            "rule_type": row.rule_type,
            "definition": row.rule_definition,
            "version_number": int(row.version_number),
            "valid_from": str(row.valid_from),
            "valid_to": str(row.valid_to) if row.valid_to else None,
            "change_type": row.change_type,
            "commit_hash": row.commit_hash
        }
        
        return json.dumps(rule_info, indent=2)
        
    except Exception as e:
        logger.error(f"Error getting rule definition: {e}", exc_info=True)
        return json.dumps({"error": f"Query failed: {str(e)}"})


@function_tool
def analyze_drop_trends(
    pipeline_name: str,
    step_name: str = None,
    rule_name: str = None,
    days_back: int = 30
) -> str:
    """
    Analyze drop rate trends over time to identify patterns and anomalies.
    
    Args:
        pipeline_name: Pipeline to analyze
        step_name: Specific step (optional)
        rule_name: Specific rule (optional)
        days_back: Number of days to analyze (default: 30)
    
    Returns:
        Trend analysis with statistics and anomaly detection
    """
    logger.info(f"Analyzing drop trends: pipeline={pipeline_name}, days={days_back}")
    
    spark = get_spark_session()
    if not spark:
        return json.dumps({"error": "Unable to connect to Spark session"})
    
    try:
        cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        query = f"""
        SELECT 
            DATE(run_timestamp) as run_date,
            step_name,
            rule_name,
            AVG(drop_percentage) as avg_drop_pct,
            MAX(drop_percentage) as max_drop_pct,
            MIN(drop_percentage) as min_drop_pct,
            STDDEV(drop_percentage) as stddev_drop_pct,
            COUNT(*) as run_count,
            SUM(dropped_records) as total_dropped,
            MAX(is_first_run_after_change) as had_code_change
        FROM pipeline_run_metrics
        WHERE pipeline_name = '{pipeline_name}'
          AND DATE(run_timestamp) >= '{cutoff_date}'
        """
        
        if step_name:
            query += f" AND step_name = '{step_name}'"
        
        if rule_name:
            query += f" AND rule_name = '{rule_name}'"
        
        query += """
        GROUP BY DATE(run_timestamp), step_name, rule_name
        ORDER BY run_date DESC, step_name, rule_name
        """
        
        df = spark.sql(query)
        results = df.collect()
        
        if not results:
            return json.dumps({
                "message": f"No data found for the last {days_back} days",
                "pipeline": pipeline_name
            })
        
        trends = []
        for row in results:
            trends.append({
                "date": str(row.run_date),
                "step": row.step_name,
                "rule": row.rule_name,
                "avg_drop_pct": round(float(row.avg_drop_pct), 2),
                "max_drop_pct": round(float(row.max_drop_pct), 2),
                "min_drop_pct": round(float(row.min_drop_pct), 2),
                "stddev": round(float(row.stddev_drop_pct), 2) if row.stddev_drop_pct else 0,
                "run_count": int(row.run_count),
                "total_dropped": int(row.total_dropped),
                "had_code_change": bool(row.had_code_change)
            })
        
        # Calculate overall statistics
        all_avgs = [t["avg_drop_pct"] for t in trends]
        overall_mean = sum(all_avgs) / len(all_avgs)
        
        return json.dumps({
            "pipeline": pipeline_name,
            "period": f"Last {days_back} days",
            "overall_avg_drop_pct": round(overall_mean, 2),
            "data_points": len(trends),
            "trends": trends
        }, indent=2)
        
    except Exception as e:
        logger.error(f"Error analyzing trends: {e}", exc_info=True)
        return json.dumps({"error": f"Analysis failed: {str(e)}"})


@function_tool
def correlate_code_changes_with_drops(
    pipeline_name: str,
    days_back: int = 30,
    threshold_change: float = 5.0
) -> str:
    """
    Correlate code changes with drop rate changes to identify impact.
    
    Args:
        pipeline_name: Pipeline to analyze
        days_back: Number of days to look back (default: 30)
        threshold_change: Minimum drop percentage change to flag (default: 5.0)
    
    Returns:
        Analysis showing which code changes correlated with drop rate changes
    """
    logger.info(f"Correlating code changes with drops: pipeline={pipeline_name}")
    
    spark = get_spark_session()
    if not spark:
        return json.dumps({"error": "Unable to connect to Spark session"})
    
    try:
        cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        query = f"""
        SELECT 
            m.run_id,
            m.run_timestamp,
            m.step_name,
            m.rule_name,
            m.drop_percentage,
            m.drop_percentage_7day_avg,
            m.drop_percentage_30day_avg,
            m.is_first_run_after_change,
            m.days_since_rule_changed,
            c.change_id,
            c.change_type,
            c.diff_summary,
            c.commit_hash,
            c.author
        FROM pipeline_run_metrics m
        LEFT JOIN pipeline_rule_changes c ON m.recent_change_id = c.change_id
        WHERE m.pipeline_name = '{pipeline_name}'
          AND DATE(m.run_timestamp) >= '{cutoff_date}'
          AND m.is_first_run_after_change = true
        ORDER BY m.run_timestamp DESC
        """
        
        df = spark.sql(query)
        results = df.collect()
        
        if not results:
            return json.dumps({
                "message": f"No code changes found in the last {days_back} days",
                "pipeline": pipeline_name
            })
        
        correlations = []
        for row in results:
            drop_change_vs_7day = None
            drop_change_vs_30day = None
            
            if row.drop_percentage_7day_avg:
                drop_change_vs_7day = round(
                    float(row.drop_percentage) - float(row.drop_percentage_7day_avg), 2
                )
            
            if row.drop_percentage_30day_avg:
                drop_change_vs_30day = round(
                    float(row.drop_percentage) - float(row.drop_percentage_30day_avg), 2
                )
            
            # Only include if change exceeds threshold
            if (drop_change_vs_7day and abs(drop_change_vs_7day) >= threshold_change) or \
               (drop_change_vs_30day and abs(drop_change_vs_30day) >= threshold_change):
                
                correlations.append({
                    "run_id": row.run_id,
                    "timestamp": str(row.run_timestamp),
                    "step": row.step_name,
                    "rule": row.rule_name,
                    "current_drop_pct": float(row.drop_percentage),
                    "baseline_7day": float(row.drop_percentage_7day_avg) if row.drop_percentage_7day_avg else None,
                    "baseline_30day": float(row.drop_percentage_30day_avg) if row.drop_percentage_30day_avg else None,
                    "change_vs_7day": drop_change_vs_7day,
                    "change_vs_30day": drop_change_vs_30day,
                    "change_type": row.change_type,
                    "diff_summary": row.diff_summary,
                    "commit": row.commit_hash,
                    "author": row.author,
                    "impact": "high" if abs(drop_change_vs_7day or 0) >= threshold_change * 2 else "moderate"
                })
        
        return json.dumps({
            "pipeline": pipeline_name,
            "period": f"Last {days_back} days",
            "threshold": threshold_change,
            "correlations_found": len(correlations),
            "correlations": correlations
        }, indent=2)
        
    except Exception as e:
        logger.error(f"Error correlating changes: {e}", exc_info=True)
        return json.dumps({"error": f"Correlation analysis failed: {str(e)}"})


@function_tool
def list_available_pipelines() -> str:
    """
    List all available pipelines in the system.
    
    Returns:
        List of pipeline names with recent activity
    """
    logger.info("Listing available pipelines")
    
    spark = get_spark_session()
    if not spark:
        return json.dumps({"error": "Unable to connect to Spark session"})
    
    try:
        query = """
        SELECT 
            pipeline_name,
            COUNT(DISTINCT run_id) as total_runs,
            MAX(run_timestamp) as last_run,
            COUNT(DISTINCT step_name) as step_count,
            COUNT(DISTINCT rule_name) as rule_count
        FROM pipeline_run_metrics
        GROUP BY pipeline_name
        ORDER BY MAX(run_timestamp) DESC
        """
        
        df = spark.sql(query)
        results = df.collect()
        
        if not results:
            return json.dumps({"message": "No pipelines found"})
        
        pipelines = []
        for row in results:
            pipelines.append({
                "name": row.pipeline_name,
                "total_runs": int(row.total_runs),
                "last_run": str(row.last_run),
                "steps": int(row.step_count),
                "rules": int(row.rule_count)
            })
        
        return json.dumps({
            "count": len(pipelines),
            "pipelines": pipelines
        }, indent=2)
        
    except Exception as e:
        logger.error(f"Error listing pipelines: {e}", exc_info=True)
        return json.dumps({"error": f"Query failed: {str(e)}"})



@function_tool
def get_pipeline_steps(pipeline_name: str) -> str:
    """
    Get all steps in a specific pipeline with their details.
    
    Args:
        pipeline_name: Name of the pipeline
    
    Returns:
        JSON list of steps with names, rule counts, and last run times
    """
    logger.info(f"Getting steps for pipeline: {pipeline_name}")
    
    spark = get_spark_session()
    if not spark:
        return json.dumps({"error": "Unable to connect to Spark session"})
    
    try:
        query = f"""
        SELECT 
            step_name,
            COUNT(DISTINCT rule_name) as rule_count,
            COUNT(DISTINCT run_id) as run_count,
            MAX(run_timestamp) as last_run,
            AVG(drop_percentage) as avg_drop_pct
        FROM pipeline_run_metrics
        WHERE pipeline_name = '{pipeline_name}'
        GROUP BY step_name
        ORDER BY MAX(run_timestamp) DESC
        """
        
        df = spark.sql(query)
        results = df.collect()
        
        if not results:
            return json.dumps({
                "error": f"No steps found for pipeline '{pipeline_name}'",
                "suggestion": "Check if the pipeline name is correct"
            })
        
        steps = []
        for row in results:
            steps.append({
                "step_name": row.step_name,
                "rule_count": int(row.rule_count),
                "run_count": int(row.run_count),
                "last_run": str(row.last_run),
                "avg_drop_pct": round(float(row.avg_drop_pct), 2) if row.avg_drop_pct else 0
            })
        
        return json.dumps({
            "pipeline": pipeline_name,
            "step_count": len(steps),
            "steps": steps
        }, indent=2)
        
    except Exception as e:
        logger.error(f"Error getting pipeline steps: {e}", exc_info=True)
        return json.dumps({"error": f"Query failed: {str(e)}"})


@function_tool
def fuzzy_search_pipelines(user_input: str, threshold: float = 0.6) -> str:
    """
    Search for pipelines using fuzzy matching to handle typos and partial names.
    
    Args:
        user_input: User's input (may be partial or misspelled pipeline name)
        threshold: Minimum similarity threshold (0.0 to 1.0, default 0.6)
    
    Returns:
        JSON list of matching pipelines with similarity scores
    """
    logger.info(f"Fuzzy searching pipelines for: '{user_input}'")
    
    from utils.fuzzy_matcher import find_best_matches, should_auto_confirm
    
    spark = get_spark_session()
    if not spark:
        return json.dumps({"error": "Unable to connect to Spark session"})
    
    try:
        # Get all available pipeline names
        query = "SELECT DISTINCT pipeline_name FROM pipeline_run_metrics ORDER BY pipeline_name"
        df = spark.sql(query)
        results = df.collect()
        
        if not results:
            return json.dumps({"error": "No pipelines found in database"})
        
        pipeline_names = [row.pipeline_name for row in results]
        
        # Perform fuzzy matching
        matches = find_best_matches(user_input, pipeline_names, threshold=threshold, max_results=5)
        
        if not matches:
            return json.dumps({
                "message": f"No pipelines found matching '{user_input}'",
                "suggestion": "Try listing all available pipelines or check your spelling",
                "available_count": len(pipeline_names)
            })
        
        # Format results
        match_results = []
        for pipeline_name, score, match_type in matches:
            match_results.append({
                "pipeline_name": pipeline_name,
                "similarity_score": round(score, 3),
                "match_type": match_type,
                "auto_confirm": should_auto_confirm(score, match_type),
                "needs_confirmation": not should_auto_confirm(score, match_type)
            })
        
        return json.dumps({
            "user_input": user_input,
            "matches_found": len(match_results),
            "matches": match_results,
            "best_match": match_results[0] if match_results else None
        }, indent=2)
        
    except Exception as e:
        logger.error(f"Error in fuzzy search: {e}", exc_info=True)
        return json.dumps({"error": f"Search failed: {str(e)}"})


# ============================================================================
# GUARDRAILS - Input and Output Validation
# ============================================================================

def input_safety_guardrail(context, agent, input_data: str | list) -> GuardrailFunctionOutput:
    """Validate user input for safety and relevance."""
    
    if isinstance(input_data, str):
        input_text = input_data
    else:
        input_text = None
        for msg in reversed(input_data):
            if isinstance(msg, dict) and msg.get("role") == "user":
                input_text = msg.get("content", "")
                break
        
        if not input_text:
            logger.warning("No user message found in input")
            return GuardrailFunctionOutput(
                tripwire_triggered=True,
                output_info={"reason": "no_user_message", "message": "No valid user message found"}
            )
    
    input_text = input_text.strip()
    logger.debug(f"Input guardrail checking: {input_text[:100]}...")
    
    if not input_text:
        logger.warning("Empty input received")
        return GuardrailFunctionOutput(
            tripwire_triggered=True,
            output_info={"reason": "empty_input", "message": "Empty input not allowed"}
        )
    
    if len(input_text) > Config.MAX_INPUT_LENGTH:
        logger.warning(f"Input too long: {len(input_text)} chars")
        return GuardrailFunctionOutput(
            tripwire_triggered=True,
            output_info={
                "reason": "input_too_long",
                "length": len(input_text),
                "message": f"Input too long ({len(input_text)} characters)"
            }
        )

    lower_input = input_text.lower()

    # Simple, focused patterns - catch the obvious 90%
    code_request_patterns = [
        # Direct code requests
        r'\b(write|generate|create|give me|show me|provide)\s+.*\s+(code|script|program|function)',
        
        # Language-specific requests
        r'\b(python|javascript|java|sql|bash|html|css|react|vue)\s+(code|script|function|component)',
        
        # Code blocks in request
        r'```\w*',
        
        # Explicit "code for/to" patterns
        r'\bcode\s+(for|to|that)\s+',
        r'\bscript\s+(for|to|that)\s+',
    ]
    
    for pattern in code_request_patterns:
        if re.search(pattern, lower_input, re.IGNORECASE):
            logger.warning(f"Code generation request detected")
            return GuardrailFunctionOutput(
                tripwire_triggered=True,
                output_info={
                    "reason": "code_generation_request",
                    "pattern": pattern,
                    "message": "Code generation requests not allowed"
                    }
            )


    # Check for prohibited patterns
    prohibited_patterns = [
        "ignore previous instructions",
        "ignore all instructions",
        "disregard above",
        "forget all",
        "system prompt",
        "jailbreak",
    ]
    
    for pattern in prohibited_patterns:
        if pattern in lower_input:
            logger.warning(f"Prohibited pattern detected: {pattern}")
            return GuardrailFunctionOutput(
                tripwire_triggered=True,
                output_info={
                    "reason": "prohibited_pattern",
                    "pattern": pattern,
                    "message": "Prohibited pattern detected in input"
                }
            )
    
    # Check for malicious commands
    malicious_keywords = [
        "execute code",
        "run command",
        "delete database",
        "drop table",
        "rm -rf",
        "sudo",
        "eval(",
        "exec(",
        "__import__",
    ]
    
    if any(keyword in lower_input for keyword in malicious_keywords):
        logger.warning("Malicious command detected in input")
        return GuardrailFunctionOutput(
            tripwire_triggered=True,
            output_info={
                "reason": "malicious_command",
                "message": "Malicious command detected in input"
            }
        )
    
    logger.debug("Input guardrail passed all checks")
    return GuardrailFunctionOutput(
        tripwire_triggered=False,
        output_info={"status": "passed", "input_length": len(input_text)}
    )


def output_safety_guardrail(context, agent, output: str) -> GuardrailFunctionOutput:
    """Validate agent output for safety and quality."""
    logger.debug(f"Output guardrail checking: {output[:100]}...")
    
    if not output or not output.strip():
        logger.warning("Empty output detected")
        return GuardrailFunctionOutput(
            tripwire_triggered=True,
            output_info={
                "reason": "empty_output",
                "message": "Empty output generated"
            }
        )
    
    lower_output = output.lower()
    
    pattern = r'\b(api_key|password|secret|token|credential|private_key|access_key)\s*[=:]\s*[\'"][^\'"]{5,}[\'"]'
    if re.search(pattern, lower_output, re.IGNORECASE):
        logger.error(f"Sensitive information in output")
        return GuardrailFunctionOutput(
            tripwire_triggered=True,
            output_info={
                "reason": "sensitive_data_in_output",
                "pattern": pattern,
                "message": "Sensitive data detected in output"
                }
        )

    if len(output) > Config.MAX_OUTPUT_LENGTH:
        logger.warning(f"Output too long: {len(output)} chars")
        return GuardrailFunctionOutput(
            tripwire_triggered=True,
            output_info={
                "reason": "output_too_long",
                "length": len(output),
                "message": "Output exceeds maximum length"
            }
        )
    
    logger.debug("Output guardrail passed all checks")
    return GuardrailFunctionOutput(
        tripwire_triggered=False,
        output_info={"status": "passed", "output_length": len(output)}
    )


# ============================================================================
# AGENT DEFINITIONS
# ============================================================================

def create_rca_agent(selected_pipeline: str = None) -> Agent:
    """
    Create the main RCA Analyzer agent with tools and guardrails.
    
    Args:
        selected_pipeline: Currently selected pipeline (if any)
    
    Returns:
        Configured Agent instance
    """
    
    base_instructions = """You are a Pipeline RCA (Root Cause Analysis) Analyzer Bot, an expert assistant specialized in analyzing data pipeline failures and performance issues.

Your role is to help users understand:
- Why pipeline runs dropped more or fewer records than usual
- How code changes impacted data processing
- Trends and patterns in drop rates over time
- Which steps or rules are causing the most data loss

Available tools and when to use them:

1. **list_available_pipelines()** - List all pipelines with activity summary
2. **fuzzy_search_pipelines(user_input)** - Find pipelines matching partial/misspelled names
3. **get_pipeline_steps(pipeline_name)** - Show all steps in a pipeline
4. **query_pipeline_metrics()** - Get detailed metrics for specific runs, steps, or rules
5. **get_rule_change_history()** - Check what code changes happened recently
6. **get_rule_definition()** - See the actual rule logic/code
7. **compare_pipeline_runs()** - Compare two specific runs side-by-side
8. **analyze_drop_trends()** - Look at historical patterns over days/weeks
9. **correlate_code_changes_with_drops()** - Find if code changes caused drop changes

Pipeline Selection Intelligence:
- When user mentions a pipeline name (even partial or misspelled), use fuzzy_search_pipelines() to find matches
- If similarity score > 0.85 and match_type is "exact" or "partial", confirm and proceed automatically
- If similarity score 0.6-0.85, ask: "Did you mean [pipeline_name]? (Yes/No)"
- If multiple good matches, present numbered list for user to choose from
- If no good match, suggest using list_available_pipelines()
- Recognize affirmations: yes, yeah, yep, correct, right, that's it, exactly
- Recognize negations: no, nope, not that one, wrong, different
- Handle uncertain responses by showing alternatives

Step Discovery:
- When user asks about steps in a pipeline, use get_pipeline_steps()
- Explain what each step does based on metrics
- Suggest which steps to investigate based on drop rates

Analysis approach:
1. Understand what the user wants to analyze (pipeline, time period, specific issue)
2. If pipeline name is unclear or partial, use fuzzy_search_pipelines() to find matches
3. Gather relevant metrics using the appropriate tools
4. Identify anomalies by comparing against baselines
5. Check if code changes correlate with metric changes
6. Provide clear, actionable insights

When analyzing drops:
- Compare current drop rates with 7-day and 30-day averages
- Check if drops occurred right after code deployments
- Look at both absolute numbers (dropped_records) and percentages (drop_percentage)
- Consider whether drops are due to stricter rules (expected) or data quality issues (unexpected)

Response style:
- Be conversational and helpful
- Present findings clearly with specific numbers
- Highlight significant changes (>5% difference typically matters)
- Explain potential root causes based on the data
- Ask clarifying questions if you need more information
- Suggest next steps for investigation
- When confirming pipeline selection, be natural: "I found 'merchant_peer_grouping' - is that what you're looking for?"

IMPORTANT: You analyze data and provide insights. You do NOT generate code, scripts, or SQL queries for users to run."""

    if selected_pipeline:
        base_instructions += f"\n\nCurrent context: User is analyzing '{selected_pipeline}' pipeline. Focus your analysis on this pipeline unless they ask about others."
    
    # Configure model settings
    model_settings = ModelSettings(
        temperature=Config.TEMPERATURE,
        max_tokens=Config.MAX_TOKENS
    )
    
    # Create agent with tools and guardrails
    agent = Agent(
        name="Pipeline RCA Analyzer Bot",
        instructions=base_instructions,
        model=Config.MODEL,
        model_settings=model_settings,
        tools=[
            list_available_pipelines,
            fuzzy_search_pipelines,
            get_pipeline_steps,
            query_pipeline_metrics,
            get_rule_change_history,
            get_rule_definition,
            compare_pipeline_runs,
            analyze_drop_trends,
            correlate_code_changes_with_drops
        ],
        input_guardrails=[
            InputGuardrail(guardrail_function=input_safety_guardrail)
        ] if Config.ENABLE_INPUT_GUARDRAILS else [],
        output_guardrails=[
            OutputGuardrail(guardrail_function=output_safety_guardrail)
        ] if Config.ENABLE_OUTPUT_GUARDRAILS else []
    )
    
    logger.info(f"RCA Agent created: pipeline={selected_pipeline}")
    return agent


def detect_pipeline_selection(user_input: str) -> str | None:
    """
    Detect if user is selecting a pipeline from their input.
    
    Args:
        user_input: User's message
    
    Returns:
        Pipeline name if detected, None otherwise
    """
    # This is a simple implementation - you can make it smarter
    # by querying actual pipeline names from the database
    lower_input = user_input.lower()
    
    # Check if user mentioned a pipeline name
    # You can extend this to query actual pipeline names dynamically
    if "pipeline" in lower_input:
        # Extract potential pipeline name (this is basic - improve as needed)
        words = user_input.split()
        for i, word in enumerate(words):
            if word.lower() == "pipeline" and i + 1 < len(words):
                potential_name = words[i + 1].strip('.,!?')
                logger.info(f"Potential pipeline detected: {potential_name}")
                return potential_name
    
    return None