# config.py - Configuration management
import os
from dotenv import load_dotenv

load_dotenv()

# fixing asyncio logs from litellm
os.environ["LITELLM_LOGGING"] = "False"
os.environ["LITELLM_DISABLE_LOGGING"] = "True"

from agents.extensions.models.litellm_model import LitellmModel
from agents import set_tracing_disabled

set_tracing_disabled(True)

class Config:
    """Application configuration for RCA Analyzer Bot"""

    # LLM settings
    LLM_API_KEY = os.environ.get("DATABRICKS_TOKEN")
    LLM_MODEL = os.getenv("LLM_MODEL", "databricks/databricks-gpt-oss-20b")

    MODEL = LitellmModel(
        model=LLM_MODEL,
        api_key=LLM_API_KEY
    )
    
    # Application Settings
    APP_NAME = "RCA Analyzer Bot"
    APP_VERSION = "1.0.0"
    
    # Session Settings
    SESSION_DB_PATH = os.getenv("SESSION_DB_PATH", "data/sessions.db")

    # Output tables (where RCA system stores data)
    METRICS_TABLE: str = "dq_poc.pipeline_run_metrics"
    RULE_DEFINITIONS_TABLE: str = "dq_poc.pipeline_rule_definitions"
    RULE_CHANGES_TABLE: str = "dq_poc.pipeline_rule_changes"
    RCA_REPORTS_TABLE: str = "dq_poc.pipeline_rca_reports"

    
    # Rate Limiting
    RATE_LIMIT_REQUESTS = int(os.getenv("RATE_LIMIT_REQUESTS", "20"))
    RATE_LIMIT_WINDOW = int(os.getenv("RATE_LIMIT_WINDOW", "60"))
    
    # Guardrails
    ENABLE_INPUT_GUARDRAILS = os.getenv("ENABLE_INPUT_GUARDRAILS", "true").lower() == "true"
    ENABLE_OUTPUT_GUARDRAILS = os.getenv("ENABLE_OUTPUT_GUARDRAILS", "true").lower() == "true"
    MAX_INPUT_LENGTH = int(os.getenv("MAX_INPUT_LENGTH", "500"))
    MAX_OUTPUT_LENGTH = int(os.getenv("MAX_OUTPUT_LENGTH", "10000"))
    
    # Model Settings
    TEMPERATURE = float(os.getenv("TEMPERATURE", "0.7"))
    MAX_TOKENS = int(os.getenv("MAX_TOKENS", "2000"))
    TIMEOUT = int(os.getenv("LLM_TIMEOUT", "30"))
    
    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE = os.getenv("LOG_FILE", "logs/rca_bot.log")
    LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", "10485760"))  # 10MB
    LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "5"))
    
    @classmethod
    def validate(cls):
        """Validate critical configuration"""
        return True