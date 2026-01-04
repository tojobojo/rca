"""
Main pipeline execution
"""

import subprocess
import sys

def install_packages():
    try:
        import openai
        return
    except ImportError:
        pass
    
    packages = [
        "openai>=2.14.0",           # Latest: v2.14.0 (Dec 19, 2025)
        "openai-agents>=0.6.4",     # Latest: v0.6.4 (Dec 2025)
        "pydantic>=2.12.5",         # Latest: v2.12.5 (Nov 26, 2025)
        "litellm>=1.80.11",         # Latest: v1.80.11 (Dec 22, 2025)
        "structlog>=25.5.0",         # Latest: v25.5.0 (Oct 27, 2025)
        "sendgrid>=6.12.5"
    ]
    
    print("Installing required packages...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade"] + packages)
    print("Installation complete!")

# Install packages
install_packages()


import os
import sys
import re
from dotenv import load_dotenv
from datetime import datetime
from typing import Dict, Any
from config.settings import Config
from core.spark_manager import SparkManager
from metrics.calculator import MetricsCalculator
from rca.analyzer import RCAAnalyzer
from rca.report_generator import ReportGenerator
from notifications.email_sender import EmailSender
from utils.logging_config import setup_logging

load_dotenv()
logger = setup_logging("main")
logger.info("Loaded environment variables from default .env file")

class PipelineRCAOrchestrator:
    """
    Main orchestrator!
    Just coordinates between components.
    """
    
    def __init__(self):
        self.metrics_calculator = MetricsCalculator()
        self.rca_analyzer = RCAAnalyzer()
        self.report_generator = ReportGenerator()
        self.email_sender = EmailSender()
        self.email_to_default = re.split(r'[;,]\s*', Config.rca.EMAIL_TO_DEFAULT) if Config.rca.EMAIL_TO_DEFAULT else []
        
    def run(
        self,
        run_id: str,
        pipeline_name: str,
    ) -> Dict[str, Any]:
        """
        Execute complete metrics + RCA pipeline.
        
        SIMPLIFIED INTERFACE - Only needs run_id and pipeline_name!
        Everything else is handled internally.
        
        Args:
            run_id: Unique run identifier (from your audit table)
            pipeline_name: Pipeline name (from your audit table)
            
        Returns:
            Complete results including metrics and RCA
        """
        logger.info("="*80)
        logger.info(f"Starting Pipeline RCA Orchestrator")
        logger.info(f"Pipeline: {pipeline_name}")
        logger.info(f"Run ID: {run_id}")
        logger.info("="*80)
        
        try:
            # ═══════════════════════════════════════════════════════
            # PHASE 1: Process Metrics (Extract + Calculate + Enrich)
            # ═══════════════════════════════════════════════════════
            logger.info("PHASE 1: Processing metrics...")
            
            metrics_result = self.metrics_calculator.process_pipeline_run(
                run_id=run_id,
                pipeline_name=pipeline_name
            )
            
            logger.info(f"✓ Metrics processed ({metrics_result['records_processed']} records)")
            
            # ═══════════════════════════════════════════════════════
            # PHASE 2: Run Automated RCA Analysis
            # ═══════════════════════════════════════════════════════
            logger.info("PHASE 2: Running RCA analysis...")
            
            analysis_results = self.rca_analyzer.analyze_run(
                run_id=run_id,
                pipeline_name=pipeline_name
            )
            
            severity = analysis_results['severity']
            logger.info(f"✓ RCA analysis completed. Severity: {severity.upper()}")
            
            # ═══════════════════════════════════════════════════════
            # PHASE 3: Generate Human-Readable Summary
            # ═══════════════════════════════════════════════════════
            logger.info("PHASE 3: Generating RCA summary...")
            
            summary = self.report_generator.generate_summary(analysis_results)
            
            logger.info("✓ RCA summary generated")
            
            # ═══════════════════════════════════════════════════════
            # PHASE 4: Send Email Notification (if needed)
            # ═══════════════════════════════════════════════════════
            
            should_send_email = (
                Config.rca.SEND_EMAILS and
                (not Config.rca.ONLY_SEND_WARNINGS or severity in ['warning', 'critical'])
            )
            
            if should_send_email and self.email_to_default:
                logger.info("PHASE 4: Sending email notification...")
                
                chatbot_link = self._generate_chatbot_link(pipeline_name, run_id)
                
                self.email_sender.send_rca_email(
                    to_emails=self.email_to_default,
                    pipeline_name=pipeline_name,
                    severity=severity,
                    summary=summary,
                    raw_data=analysis_results,
                    chatbot_link=chatbot_link
                )
                
                logger.info(f"✓ Email sent to {len(self.email_to_default)} recipients")
            else:
                logger.info(f"PHASE 4: Skipping email (severity={severity})")
            
            # ═══════════════════════════════════════════════════════
            # PHASE 5: Store RCA Report (optional)
            # ═══════════════════════════════════════════════════════
            
            if Config.rca.STORE_RCA_REPORTS:
                logger.info("PHASE 5: Storing RCA report...")
                self._store_rca_report(run_id, analysis_results, summary)
                logger.info("✓ RCA report stored")
            
            # ═══════════════════════════════════════════════════════
            # Complete
            # ═══════════════════════════════════════════════════════
            
            logger.info("="*80)
            logger.info(f"✓ Pipeline RCA Orchestrator completed successfully")
            logger.info(f"Severity: {severity.upper()}")
            logger.info("="*80)
            
            return {
                'success': True,
                'run_id': run_id,
                'pipeline_name': pipeline_name,
                'severity': severity,
                'summary': summary,
                'analysis': analysis_results,
                'metrics_records': metrics_result['records_processed']
            }
            
        except Exception as e:
            logger.error("="*80)
            logger.error(f"✗ Pipeline RCA Orchestrator FAILED")
            logger.error(f"Error: {str(e)}")
            logger.error("="*80)
            logger.exception("Full traceback:")
            
            return {
                'success': False,
                'run_id': run_id,
                'pipeline_name': pipeline_name,
                'error': str(e)
            }
    
    def _generate_chatbot_link(self, pipeline_name: str, run_id: str) -> str:
        """Generate deep link to chatbot with context"""
        base_url = os.getenv("CHATBOT_URL", "https://your-chatbot-url.com")
        return f"{base_url}?pipeline={pipeline_name}&run_id={run_id}"
    
    def _store_rca_report(
        self,
        run_id: str,
        analysis_results: Dict[str, Any],
        summary: str
    ):
        """Store RCA report in Delta table for chatbot reference"""
        import json
        
        spark = SparkManager.get_session()
        
        report_data = [{
            'run_id': run_id,
            'pipeline_name': analysis_results['pipeline_name'],
            'analysis_timestamp': datetime.now(),
            'severity': analysis_results['severity'],
            'summary_text': summary,
            'analysis_json': json.dumps(analysis_results),
            'metrics_summary_json': json.dumps(analysis_results['metrics_summary'])
        }]
        
        df = spark.createDataFrame(report_data)
        
        from delta.tables import DeltaTable
        # Check if table exists
        if SparkManager.table_exists(Config.db.RCA_REPORTS_TABLE):
            # Table exists - use MERGE (upsert)
            delta_table = DeltaTable.forName(spark, Config.db.RCA_REPORTS_TABLE)
            
            delta_table.alias("target").merge(
                df.alias("source"),
                "target.run_id = source.run_id"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
            
            logger.info(f"✓")
        else:
            df.write.format("delta") \
                .mode("append") \
                .saveAsTable(Config.db.RCA_REPORTS_TABLE)
            logger.info(f"✓")



def main():
    """
    Main entry point for the pipeline
    """
    # Validate configuration
    try:
        Config.validate()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    
    logger.info("Pipeline RCA System initialized and ready")

    run_id = "run_024"
    pipeline_name = "merchant_peer_grouping_pipeline"

    orchestrator = PipelineRCAOrchestrator()

    result = orchestrator.run(
        run_id=run_id,
        pipeline_name=pipeline_name,
    )

    if result['success']:
        print(f"✓ Success!")
        print(f"  Severity: {result['severity']}")
        print(f"  Summary: {result['summary'][:200]}...")
    else:
        print(f"✗ Failed: {result['error']}")


if __name__ == "__main__":
    main()