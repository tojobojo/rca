import json
from typing import Dict, Any
from openai import OpenAI
from config.settings import Config
from core.exceptions import RCAAnalysisError
from utils.logging_config import setup_logging

logger = setup_logging("report_generator")


class ReportGenerator:
	"""Generate human-readable RCA reports using LLM"""
    
	def __init__(self):
		print("Config.rca.LLM_API_KEY", Config.rca.LLM_API_KEY)
		print("Config.rca.LLM_BASE_URL", Config.rca.LLM_BASE_URL)
		print("Config.rca.LLM_MODEL", Config.rca.LLM_MODEL)
		self.client = OpenAI(api_key=Config.rca.LLM_API_KEY, base_url=Config.rca.LLM_BASE_URL)
		self.model = Config.rca.LLM_MODEL
		self.temperature = Config.rca.LLM_TEMPERATURE
		self.max_tokens = Config.rca.LLM_MAX_TOKENS
    
	def generate_summary(self, analysis_results: Dict[str, Any]) -> str:
		"""
		Generate narrative RCA summary using LLM
        
		Args:
			analysis_results: Complete analysis results
            
		Returns:
			Human-readable summary text
		"""
		logger.info(f"Generating RCA summary for {analysis_results['pipeline_name']}")
        
		try:
			prompt = self._build_prompt(analysis_results)
            
			response = self.client.chat.completions.create(
				model=self.model,
				messages=[
					{
						"role": "system",
						"content": self._get_system_prompt()
					},
					{
						"role": "user",
						"content": prompt
					}
				],
				temperature=self.temperature,
				max_tokens=self.max_tokens
			)
            
			content = response.choices[0].message.content

			summary = "\n".join(
				block["text"]
				for block in content
				if block["type"] == "text"
			)
            
			logger.info("RCA summary generated successfully")
			logger.debug(f"Summary preview: {summary[:200]}...")
            
			return summary
            
		except Exception as e:
			logger.error(f"Failed to generate RCA summary: {e}", exc_info=True)
			# Return fallback summary
			return self._generate_fallback_summary(analysis_results)
    
	def _get_system_prompt(self) -> str:
		"""Get system prompt for LLM"""
		return """You are an expert data pipeline analyst specializing in root cause analysis.

Your job is to take structured analysis results and generate clear, actionable RCA summaries for technical stakeholders.

Guidelines:
- Start with a concise executive summary (2-3 sentences)
- Explain the root cause clearly with supporting data
- Reference specific metrics, steps, and rules
- Provide actionable recommendations
- Adjust tone based on severity:
  * CRITICAL: Urgent, requires immediate action
  * WARNING: Concerned, needs attention
  * NORMAL: Informative, monitoring recommended
- Keep it concise but thorough (aim for 300-500 words)
- Use professional but accessible language
- Format for email readability (use paragraphs, not bullet points)

Do NOT:
- Make up data not provided
- Speculate beyond the evidence
- Use overly technical jargon
- Include code snippets"""
    
	def _build_prompt(self, analysis: Dict[str, Any]) -> str:
		"""Build prompt for LLM"""
        
		metrics = analysis['metrics_summary']
		severity = analysis['severity']
        
		prompt = f"""Generate a comprehensive RCA summary for this pipeline run:

PIPELINE INFORMATION:
- Pipeline: {analysis['pipeline_name']}
- Run ID: {analysis['run_id']}
- Timestamp: {analysis['timestamp']}
- Severity: {severity.upper()}

METRICS SUMMARY:
- Total Input Records: {metrics['total_input_records']:,}
- Total Dropped Records: {metrics['total_dropped_records']:,}
- Current Drop Rate: {metrics['current_drop_pct']}%
- Previous Drop Rate: {metrics.get('previous_drop_pct', 'N/A')}%
- 7-Day Baseline: {metrics['baseline_7day_avg']}%
- 30-Day Baseline: {metrics['baseline_30day_avg']}%
- Change vs Previous: {metrics.get('drop_delta', 0):+.1f}%

"""
        
		# Add comparison details
		if analysis['comparison']:
			prompt += "\nTOP CHANGES VS PREVIOUS RUN:\n"
			for comp in analysis['comparison'][:5]:  # Top 5
				prompt += f"- {comp['step']} / {comp['rule']}: {comp['current_drop_pct']}% (was {comp['previous_drop_pct']}%, {comp['drop_delta']:+.1f}%)\n"
        
		# Add anomalies
		if analysis['anomalies']:
			prompt += "\nSTATISTICAL ANOMALIES DETECTED:\n"
			for anom in analysis['anomalies']:
				prompt += f"- {anom['step']} / {anom['rule']}: {anom['current_drop_pct']}% (baseline: {anom['baseline_avg']}%, z-score: {anom['z_score']})\n"
        
		# Add code changes
		if analysis['correlations']:
			prompt += "\nCODE CHANGES CORRELATION:\n"
			for corr in analysis['correlations']:
				prompt += f"- {corr['rule']}: {corr['change_summary']} (impact: {corr['impact']}, delta: {corr['change_delta']:+.1f}%)\n"
				prompt += f"  Commit: {corr['commit_hash'][:8]} by {corr['author']}\n"
        
		# Add step analysis
		if analysis['step_analysis']:
			prompt += "\nSTEP-LEVEL ANALYSIS:\n"
			for step in analysis['step_analysis'][:5]:  # Top 5 steps
				prompt += f"- {step['step_name']}: {step['drop_percentage']}% drop rate ({step['dropped_records']:,} records dropped)\n"
        
		prompt += "\n\nGenerate a professional RCA summary with:\n"
		prompt += "1. Executive Summary (what happened)\n"
		prompt += "2. Root Cause Analysis (why it happened)\n"
		prompt += "3. Detailed Findings (supporting evidence)\n"
		prompt += "4. Recommendations (what to do next)\n"
        
		return prompt
    
	def _generate_fallback_summary(self, analysis: Dict[str, Any]) -> str:
		"""Generate simple fallback summary if LLM fails"""
        
		metrics = analysis['metrics_summary']
		severity = analysis['severity']
        
		summary = f"""RCA Summary - {analysis['pipeline_name']}

Severity: {severity.upper()}

The pipeline processed {metrics['total_input_records']:,} records and dropped {metrics['total_dropped_records']:,} records ({metrics['current_drop_pct']}%).
"""
        
		if metrics.get('drop_delta'):
			summary += f"This is a {metrics['drop_delta']:+.1f}% change compared to the previous run.\n"
        
		if analysis['anomalies']:
			summary += f"\n{len(analysis['anomalies'])} statistical anomalies were detected.\n"
        
		if analysis['correlations']:
			summary += f"\n{len(analysis['correlations'])} code changes may have impacted drop rates.\n"
        
		summary += "\nPlease review the detailed metrics for more information."
        
		return summary

