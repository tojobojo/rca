from typing import Dict, Any
from config.settings import Config
from utils.logging_config import setup_logging

logger = setup_logging("severity_classifier")


class SeverityClassifier:
	"""Classify RCA severity based on analysis results"""
    
	def __init__(self):
		self.warning_threshold = Config.rca.WARNING_THRESHOLD_PCT
		self.critical_threshold = Config.rca.CRITICAL_THRESHOLD_PCT
    
	def classify(self, analysis: Dict[str, Any]) -> str:
		"""
		Classify severity: 'normal', 'warning', or 'critical'
        
		Args:
			analysis: Analysis results dict
            
		Returns:
			Severity level
		"""
		logger.debug("Classifying severity")
        
		try:
			# Extract key metrics
			drop_delta = analysis.get('metrics_summary', {}).get('drop_delta', 0)
			max_step_delta = max(
				[s.get('drop_delta', 0) for s in analysis.get('step_analysis', [])],
				default=0
			)
			has_anomalies = len(analysis.get('anomalies', [])) > 0
			has_high_impact_changes = any(
				c.get('impact') == 'high' 
				for c in analysis.get('correlations', [])
			)
            
			# Classification logic
			if drop_delta >= self.critical_threshold or max_step_delta >= self.critical_threshold:
				severity = 'critical'
			elif drop_delta >= self.warning_threshold or max_step_delta >= self.warning_threshold:
				severity = 'warning'
			elif has_anomalies or has_high_impact_changes:
				severity = 'warning'
			else:
				severity = 'normal'
            
			logger.info(f"Classified as: {severity.upper()}")
			return severity
            
		except Exception as e:
			logger.error(f"Severity classification failed: {e}")
			return 'unknown'
