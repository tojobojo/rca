import json
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from config.settings import Config
from core.spark_manager import SparkManager
from core.exceptions import RCAAnalysisError
from rca.severity_classifier import SeverityClassifier
from utils.logging_config import setup_logging

logger = setup_logging("rca_analyzer")


class RCAAnalyzer:
    """Automated RCA analysis engine"""

    def __init__(self):
        self.severity_classifier = SeverityClassifier()

    def analyze_run(
        self,
        run_id: str,
        pipeline_name: str
    ) -> Dict[str, Any]:
        """
        Perform complete RCA analysis for a pipeline run

        Args:
            run_id: Run ID to analyze
            pipeline_name: Pipeline name

        Returns:
            Complete analysis results dictionary
        """
        logger.info(f"Starting RCA analysis for {pipeline_name}, run_id={run_id}")

        try:
            # Phase 1: Data Collection
            current_metrics = self._get_current_run_metrics(run_id, pipeline_name)
            previous_metrics = self._get_previous_run_metrics(pipeline_name, run_id)
            code_changes = self._get_recent_code_changes(pipeline_name)

            # Phase 2: Analysis
            comparison = self._compare_runs(current_metrics, previous_metrics)
            anomalies = self._detect_anomalies(current_metrics)
            correlations = self._correlate_code_changes(
                current_metrics, code_changes
            )
            step_analysis = self._analyze_by_step(current_metrics)

            # Compile results
            analysis_results = {
                'run_id': run_id,
                'pipeline_name': pipeline_name,
                'timestamp': datetime.now().isoformat(),
                'metrics_summary': self._create_metrics_summary(current_metrics, previous_metrics),
                'comparison': comparison,
                'anomalies': anomalies,
                'code_changes': code_changes,
                'correlations': correlations,
                'step_analysis': step_analysis
            }

            # Classify severity
            analysis_results['severity'] = self.severity_classifier.classify(analysis_results)

            logger.info(f"RCA analysis completed. Severity: {analysis_results['severity']}")
            return analysis_results

        except Exception as e:
            logger.error(f"RCA analysis failed: {e}", exc_info=True)
            raise RCAAnalysisError(f"RCA analysis failed: {e}")

    def _get_current_run_metrics(self, run_id: str, pipeline_name: str) -> List[Dict]:
        """Get metrics for current run"""
        spark = SparkManager.get_session()
        df = (
            spark.table(Config.db.METRICS_TABLE)
            .filter((F.col("run_id") == run_id) & (F.col("pipeline_name") == pipeline_name))
        )

        return [row.asDict() for row in df.collect()]

    def _get_previous_run_metrics(self, pipeline_name: str, current_run_id: str) -> Optional[List[Dict]]:
        """Get metrics for previous run"""
        spark = SparkManager.get_session()
        df = (
            spark.table(Config.db.METRICS_TABLE)
            .filter((F.col("pipeline_name") == pipeline_name) & (F.col("run_id") != current_run_id))
            .orderBy(F.col("run_timestamp").desc())
            .limit(1000)
        )

        rows = df.collect()

        if not rows:
            logger.warning("No previous run found for comparison")
            return None

        # Get the most recent previous run
        previous_run_id = rows[0].run_id
        return [row.asDict() for row in rows if row.run_id == previous_run_id]

    def _get_recent_code_changes(self, pipeline_name: str) -> List[Dict]:
        """Get recent code changes"""
        window_hours = Config.rca.CODE_CHANGE_WINDOW_HOURS

        spark = SparkManager.get_session()
        changes_tbl = spark.table(Config.db.RULE_CHANGES_TABLE)

        df = (
            changes_tbl
            .filter(
                (F.col("pipeline_name") == pipeline_name)
                & (F.unix_timestamp(F.col("change_timestamp")) >= F.unix_timestamp(F.current_timestamp()) - (window_hours * 3600))
            )
            .orderBy(F.col("change_timestamp").desc())
        )

        return [row.asDict() for row in df.collect()]

    def _create_metrics_summary(
        self,
        current: List[Dict],
        previous: Optional[List[Dict]]
    ) -> Dict[str, Any]:
        """Create high-level metrics summary"""

        # Current totals
        total_input = sum(m['input_records'] for m in current)
        total_dropped = sum(m['dropped_records'] for m in current)
        current_drop_pct = (total_dropped / total_input * 100) if total_input > 0 else 0

        # Baseline averages
        avg_7day = sum(m.get('drop_percentage_7day_avg', 0) or 0 for m in current) / len(current) if current else 0
        avg_30day = sum(m.get('drop_percentage_30day_avg', 0) or 0 for m in current) / len(current) if current else 0

        summary = {
            'total_input_records': total_input,
            'total_output_records': sum(m['output_records'] for m in current),
            'total_dropped_records': total_dropped,
            'current_drop_pct': round(current_drop_pct, 2),
            'baseline_7day_avg': round(avg_7day, 2),
            'baseline_30day_avg': round(avg_30day, 2)
        }

        # Compare with previous
        if previous:
            prev_total_input = sum(m['input_records'] for m in previous)
            prev_total_dropped = sum(m['dropped_records'] for m in previous)
            prev_drop_pct = (prev_total_dropped / prev_total_input * 100) if prev_total_input > 0 else 0

            summary['previous_drop_pct'] = round(prev_drop_pct, 2)
            summary['drop_delta'] = round(current_drop_pct - prev_drop_pct, 2)
        else:
            summary['previous_drop_pct'] = None
            summary['drop_delta'] = 0

        return summary

    def _compare_runs(
        self,
        current: List[Dict],
        previous: Optional[List[Dict]]
    ) -> List[Dict]:
        """Compare current run with previous run"""

        if not previous:
            return []

        comparisons = []

        # Create lookup for previous metrics
        prev_lookup = {
            f"{m['step_name']}:{m['rule_name']}": m
            for m in previous
        }

        for curr_metric in current:
            key = f"{curr_metric['step_name']}:{curr_metric['rule_name']}"
            prev_metric = prev_lookup.get(key)

            if prev_metric:
                delta = curr_metric['drop_percentage'] - prev_metric['drop_percentage']
                comparisons.append({
                    'step': curr_metric['step_name'],
                    'rule': curr_metric['rule_name'],
                    'current_drop_pct': round(curr_metric['drop_percentage'], 2),
                    'previous_drop_pct': round(prev_metric['drop_percentage'], 2),
                    'drop_delta': round(delta, 2),
                    'status': 'increased' if delta > 0 else 'decreased' if delta < 0 else 'unchanged'
                })

        # Sort by absolute delta (largest changes first)
        comparisons.sort(key=lambda x: abs(x['drop_delta']), reverse=True)

        return comparisons

    def _detect_anomalies(self, current: List[Dict]) -> List[Dict]:
        """Detect statistical anomalies"""

        anomalies = []
        threshold_stddev = Config.rca.ANOMALY_THRESHOLD_STDDEV

        for metric in current:
            baseline_avg = metric.get('drop_percentage_7day_avg')
            baseline_stddev = metric.get('drop_percentage_stddev')

            if baseline_avg is not None and baseline_stddev is not None and baseline_stddev > 0:
                z_score = (metric['drop_percentage'] - baseline_avg) / baseline_stddev

                if abs(z_score) >= threshold_stddev:
                    anomalies.append({
                        'step': metric['step_name'],
                        'rule': metric['rule_name'],
                        'current_drop_pct': round(metric['drop_percentage'], 2),
                        'baseline_avg': round(baseline_avg, 2),
                        'z_score': round(z_score, 2),
                        'severity': 'high' if abs(z_score) >= 3 else 'moderate'
                    })

        return anomalies

    def _correlate_code_changes(
        self,
        current: List[Dict],
        code_changes: List[Dict]
    ) -> List[Dict]:
        """Correlate code changes with metric changes"""

        if not code_changes:
            return []

        correlations = []

        for metric in current:
            if metric.get('is_first_run_after_change'):
                # Find matching code change
                matching_change = next(
                    (c for c in code_changes if c['rule_name'] == metric['rule_name']),
                    None
                )

                if matching_change:
                    baseline_7day = metric.get('drop_percentage_7day_avg', 0) or 0
                    change_delta = metric['drop_percentage'] - baseline_7day

                    if abs(change_delta) >= Config.rca.WARNING_THRESHOLD_PCT:
                        correlations.append({
                            'step': metric['step_name'],
                            'rule': metric['rule_name'],
                            'current_drop_pct': round(metric['drop_percentage'], 2),
                            'baseline_7day': round(baseline_7day, 2),
                            'change_delta': round(change_delta, 2),
                            'change_type': matching_change.get('change_type'),
                            'change_summary': matching_change.get('diff_summary'),
                            'commit_hash': matching_change.get('commit_hash'),
                            'author': matching_change.get('author'),
                            'impact': 'high' if abs(change_delta) >= Config.rca.CRITICAL_THRESHOLD_PCT else 'moderate'
                        })

        return correlations

    def _analyze_by_step(self, current: List[Dict]) -> List[Dict]:
        """Analyze metrics aggregated by step"""

        from collections import defaultdict

        step_data = defaultdict(lambda: {
            'input': 0,
            'output': 0,
            'dropped': 0,
            'rules': []
        })

        for metric in current:
            step = metric['step_name']
            step_data[step]['input'] += metric['input_records']
            step_data[step]['output'] += metric['output_records']
            step_data[step]['dropped'] += metric['dropped_records']
            step_data[step]['rules'].append(metric['rule_name'])

        step_analysis = []
        for step_name, data in step_data.items():
            drop_pct = (data['dropped'] / data['input'] * 100) if data['input'] > 0 else 0

            step_analysis.append({
                'step_name': step_name,
                'input_records': data['input'],
                'output_records': data['output'],
                'dropped_records': data['dropped'],
                'drop_percentage': round(drop_pct, 2),
                'rule_count': len(data['rules'])
            })

        # Sort by drop percentage
        step_analysis.sort(key=lambda x: x['drop_percentage'], reverse=True)

        return step_analysis
import json
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from config.settings import Config
from core.spark_manager import SparkManager
from core.exceptions import RCAAnalysisError
from rca.severity_classifier import SeverityClassifier
from utils.logging_config import setup_logging

logger = setup_logging("rca_analyzer")


class RCAAnalyzer:
    """Automated RCA analysis engine"""
    
    def __init__(self):
        self.severity_classifier = SeverityClassifier()
    
    def analyze_run(
        self,
        run_id: str,
        pipeline_name: str
    ) -> Dict[str, Any]:
        """
        Perform complete RCA analysis for a pipeline run
        
        Args:
            run_id: Run ID to analyze
            pipeline_name: Pipeline name
            
        Returns:
            Complete analysis results dictionary
        """
        logger.info(f"Starting RCA analysis for {pipeline_name}, run_id={run_id}")
        
        try:
            # Phase 1: Data Collection
            current_metrics = self._get_current_run_metrics(run_id, pipeline_name)
            previous_metrics = self._get_previous_run_metrics(pipeline_name, run_id)
            code_changes = self._get_recent_code_changes(pipeline_name)
            
            # Phase 2: Analysis
            comparison = self._compare_runs(current_metrics, previous_metrics)
            anomalies = self._detect_anomalies(current_metrics)
            correlations = self._correlate_code_changes(
                current_metrics, code_changes
            )
            step_analysis = self._analyze_by_step(current_metrics)
            
            # Compile results
            analysis_results = {
                'run_id': run_id,
                'pipeline_name': pipeline_name,
                'timestamp': datetime.now().isoformat(),
                'metrics_summary': self._create_metrics_summary(current_metrics, previous_metrics),
                'comparison': comparison,
                'anomalies': anomalies,
                'code_changes': code_changes,
                'correlations': correlations,
                'step_analysis': step_analysis
            }
            
            # Classify severity
            analysis_results['severity'] = self.severity_classifier.classify(analysis_results)
            
            logger.info(f"RCA analysis completed. Severity: {analysis_results['severity']}")
            return analysis_results
            
        except Exception as e:
            logger.error(f"RCA analysis failed: {e}", exc_info=True)
            raise RCAAnalysisError(f"RCA analysis failed: {e}")
    
    def _get_current_run_metrics(self, run_id: str, pipeline_name: str) -> List[Dict]:
        """Get metrics for current run"""
        query = f"""
        SELECT *
        FROM {Config.db.METRICS_TABLE}
        WHERE run_id = '{run_id}'
          AND pipeline_name = '{pipeline_name}'
        """
        
        df = SparkManager.execute_query(query)
        return [row.asDict() for row in df.collect()]
    
    def _get_previous_run_metrics(self, pipeline_name: str, current_run_id: str) -> Optional[List[Dict]]:
        """Get metrics for previous run"""
        query = f"""
        SELECT *
        FROM {Config.db.METRICS_TABLE}
        WHERE pipeline_name = '{pipeline_name}'
          AND run_id != '{current_run_id}'
        ORDER BY run_timestamp DESC
        LIMIT 1000
        """
        
        df = SparkManager.execute_query(query)
        rows = df.collect()
        
        if not rows:
            logger.warning("No previous run found for comparison")
            return None
        
        # Get the most recent previous run
        previous_run_id = rows[0].run_id
        return [row.asDict() for row in rows if row.run_id == previous_run_id]
    
    def _get_recent_code_changes(self, pipeline_name: str) -> List[Dict]:
        """Get recent code changes"""
        window_hours = Config.rca.CODE_CHANGE_WINDOW_HOURS

        # Use unix_timestamp arithmetic for hour-based windowing in Spark
        query = f"""
        SELECT *
        FROM {Config.db.RULE_CHANGES_TABLE}
        WHERE pipeline_name = '{pipeline_name}'
            AND unix_timestamp(change_timestamp) >= unix_timestamp(current_timestamp()) - {window_hours} * 3600
        ORDER BY change_timestamp DESC
        """

        df = SparkManager.execute_query(query)
        return [row.asDict() for row in df.collect()]
    
    def _create_metrics_summary(
        self, 
        current: List[Dict], 
        previous: Optional[List[Dict]]
    ) -> Dict[str, Any]:
        """Create high-level metrics summary"""
        
        # Current totals
        total_input = sum(m['input_records'] for m in current)
        total_dropped = sum(m['dropped_records'] for m in current)
        current_drop_pct = (total_dropped / total_input * 100) if total_input > 0 else 0
        
        # Baseline averages
        avg_7day = sum(m.get('drop_percentage_7day_avg', 0) or 0 for m in current) / len(current) if current else 0
        avg_30day = sum(m.get('drop_percentage_30day_avg', 0) or 0 for m in current) / len(current) if current else 0
        
        summary = {
            'total_input_records': total_input,
            'total_output_records': sum(m['output_records'] for m in current),
            'total_dropped_records': total_dropped,
            'current_drop_pct': round(current_drop_pct, 2),
            'baseline_7day_avg': round(avg_7day, 2),
            'baseline_30day_avg': round(avg_30day, 2)
        }
        
        # Compare with previous
        if previous:
            prev_total_input = sum(m['input_records'] for m in previous)
            prev_total_dropped = sum(m['dropped_records'] for m in previous)
            prev_drop_pct = (prev_total_dropped / prev_total_input * 100) if prev_total_input > 0 else 0
            
            summary['previous_drop_pct'] = round(prev_drop_pct, 2)
            summary['drop_delta'] = round(current_drop_pct - prev_drop_pct, 2)
        else:
            summary['previous_drop_pct'] = None
            summary['drop_delta'] = 0
        
        return summary
    
    def _compare_runs(
        self, 
        current: List[Dict], 
        previous: Optional[List[Dict]]
    ) -> List[Dict]:
        """Compare current run with previous run"""
        
        if not previous:
            return []
        
        comparisons = []
        
        # Create lookup for previous metrics
        prev_lookup = {
            f"{m['step_name']}:{m['rule_name']}": m 
            for m in previous
        }
        
        for curr_metric in current:
            key = f"{curr_metric['step_name']}:{curr_metric['rule_name']}"
            prev_metric = prev_lookup.get(key)
            
            if prev_metric:
                delta = curr_metric['drop_percentage'] - prev_metric['drop_percentage']
                comparisons.append({
                    'step': curr_metric['step_name'],
                    'rule': curr_metric['rule_name'],
                    'current_drop_pct': round(curr_metric['drop_percentage'], 2),
                    'previous_drop_pct': round(prev_metric['drop_percentage'], 2),
                    'drop_delta': round(delta, 2),
                    'status': 'increased' if delta > 0 else 'decreased' if delta < 0 else 'unchanged'
                })
        
        # Sort by absolute delta (largest changes first)
        comparisons.sort(key=lambda x: abs(x['drop_delta']), reverse=True)
        
        return comparisons
    
    def _detect_anomalies(self, current: List[Dict]) -> List[Dict]:
        """Detect statistical anomalies"""
        
        anomalies = []
        threshold_stddev = Config.rca.ANOMALY_THRESHOLD_STDDEV
        
        for metric in current:
            baseline_avg = metric.get('drop_percentage_7day_avg')
            baseline_stddev = metric.get('drop_percentage_stddev')
            
            if baseline_avg is not None and baseline_stddev is not None and baseline_stddev > 0:
                z_score = (metric['drop_percentage'] - baseline_avg) / baseline_stddev
                
                if abs(z_score) >= threshold_stddev:
                    anomalies.append({
                        'step': metric['step_name'],
                        'rule': metric['rule_name'],
                        'current_drop_pct': round(metric['drop_percentage'], 2),
                        'baseline_avg': round(baseline_avg, 2),
                        'z_score': round(z_score, 2),
                        'severity': 'high' if abs(z_score) >= 3 else 'moderate'
                    })
        
        return anomalies
    
    def _correlate_code_changes(
        self, 
        current: List[Dict],
        code_changes: List[Dict]
    ) -> List[Dict]:
        """Correlate code changes with metric changes"""
        
        if not code_changes:
            return []
        
        correlations = []
        
        for metric in current:
            if metric.get('is_first_run_after_change'):
                # Find matching code change
                matching_change = next(
                    (c for c in code_changes if c['rule_name'] == metric['rule_name']),
                    None
                )
                
                if matching_change:
                    baseline_7day = metric.get('drop_percentage_7day_avg', 0) or 0
                    change_delta = metric['drop_percentage'] - baseline_7day
                    
                    if abs(change_delta) >= Config.rca.WARNING_THRESHOLD_PCT:
                        correlations.append({
                            'step': metric['step_name'],
                            'rule': metric['rule_name'],
                            'current_drop_pct': round(metric['drop_percentage'], 2),
                            'baseline_7day': round(baseline_7day, 2),
                            'change_delta': round(change_delta, 2),
                            'change_type': matching_change.get('change_type'),
                            'change_summary': matching_change.get('diff_summary'),
                            'commit_hash': matching_change.get('commit_hash'),
                            'author': matching_change.get('author'),
                            'impact': 'high' if abs(change_delta) >= Config.rca.CRITICAL_THRESHOLD_PCT else 'moderate'
                        })
        
        return correlations
    
    def _analyze_by_step(self, current: List[Dict]) -> List[Dict]:
        """Analyze metrics aggregated by step"""
        
        from collections import defaultdict
        
        step_data = defaultdict(lambda: {
            'input': 0,
            'output': 0,
            'dropped': 0,
            'rules': []
        })
        
        for metric in current:
            step = metric['step_name']
            step_data[step]['input'] += metric['input_records']
            step_data[step]['output'] += metric['output_records']
            step_data[step]['dropped'] += metric['dropped_records']
            step_data[step]['rules'].append(metric['rule_name'])
        
        step_analysis = []
        for step_name, data in step_data.items():
            drop_pct = (data['dropped'] / data['input'] * 100) if data['input'] > 0 else 0
            
            step_analysis.append({
                'step_name': step_name,
                'input_records': data['input'],
                'output_records': data['output'],
                'dropped_records': data['dropped'],
                'drop_percentage': round(drop_pct, 2),
                'rule_count': len(data['rules'])
            })
        
        # Sort by drop percentage
        step_analysis.sort(key=lambda x: x['drop_percentage'], reverse=True)
        
        return step_analysis
