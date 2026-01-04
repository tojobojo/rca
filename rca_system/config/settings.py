import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

@dataclass
class DatabaseConfig:
    """Database and table configuration"""
    # Input table (where your pipeline logs metrics)
    AUDIT_TABLE: str = "dq_poc.pipeline_step_audit"
    
    # Output tables (where RCA system stores data)
    METRICS_TABLE: str = "dq_poc.pipeline_run_metrics"
    RULE_DEFINITIONS_TABLE: str = "dq_poc.pipeline_rule_definitions"
    RULE_CHANGES_TABLE: str = "dq_poc.pipeline_rule_changes"
    RCA_REPORTS_TABLE: str = "dq_poc.pipeline_rca_reports"
    
    # CATALOG: Optional[str] = None  # Unity Catalog
    # SCHEMA: str = "pipeline_monitoring"


@dataclass
class RCAConfig:
    """RCA analysis configuration"""
    BASELINE_DAYS_SHORT: int = 7
    BASELINE_DAYS_LONG: int = 30
    ANOMALY_THRESHOLD_STDDEV: float = 2.0
    WARNING_THRESHOLD_PCT: float = 10.0
    CRITICAL_THRESHOLD_PCT: float = 50.0
    CODE_CHANGE_WINDOW_HOURS: int = 48
    
    # LLM Configuration
    LLM_API_KEY: str = os.environ.get("DATABRICKS_TOKEN")
    LLM_BASE_URL: str = os.environ.get("LLM_BASE_URL", "https://dbc-7588dbbf-6cbc.cloud.databricks.com/serving-endpoints")
    LLM_MODEL: str = os.getenv("LLM_MODEL", "databricks-gpt-oss-20b")
    LLM_TEMPERATURE: float = 0.2
    LLM_MAX_TOKENS: int = 1500
    
    # Email Configuration
    EMAIL_PROVIDER: str = os.getenv("EMAIL_PROVIDER", "sendgrid")

    SMTP_HOST: str = os.getenv("SMTP_HOST", "smtp.gmail.com")
    SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USER: str = os.getenv("SMTP_USER", "")
    SMTP_PASSWORD: str = os.getenv("SMTP_PASSWORD", "")

    SENDGRID_API_KEY: str = os.getenv("SENDGRID_API_KEY", "")

    EMAIL_FROM: str = os.getenv("EMAIL_FROM", "")
    EMAIL_TO_DEFAULT: str = os.getenv("EMAIL_TO_DEFAULT", "")
    
    # Feature Flags
    SEND_EMAILS: bool = os.getenv("SEND_EMAILS", "false").lower() == "true"
    ONLY_SEND_WARNINGS: bool = False
    STORE_RCA_REPORTS: bool = True


@dataclass
class LoggingConfig:
    """Logging configuration"""
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    LOG_FILE: Optional[str] = "logs/rca_pipeline.log"


class Config:
    """Main configuration class"""
    db = DatabaseConfig()
    rca = RCAConfig()
    logging = LoggingConfig()
    
    @classmethod
    def validate(cls):
        """Validate configuration"""
        if not cls.rca.SEND_EMAILS:
            return

        provider = cls.rca.EMAIL_PROVIDER.lower()

        if provider == "smtp":
            if not cls.rca.SMTP_USER or not cls.rca.SMTP_PASSWORD:
                raise ValueError(
                    "SMTP email selected but SMTP_USER or SMTP_PASSWORD is not configured"
                )

        elif provider == "sendgrid":
            if not cls.rca.SENDGRID_API_KEY:
                raise ValueError(
                    "SendGrid selected but SENDGRID_API_KEY is not configured"
                )

        else:
            raise ValueError(f"Unsupported EMAIL_PROVIDER: {cls.rca.EMAIL_PROVIDER}")


