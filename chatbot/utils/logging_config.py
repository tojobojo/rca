# utils/logging_config.py
"""Logging configuration for RCA Analyzer Bot"""

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from config import Config

def setup_logging():
    """Setup application logging with console and file handlers"""
    
    # Create logs directory
    log_dir = Path(Config.LOG_FILE).parent
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Create logger
    logger = logging.getLogger("rca_bot")
    logger.setLevel(getattr(logging, Config.LOG_LEVEL))
    
    # Remove existing handlers to avoid duplicates
    logger.handlers = []
    
    # Console handler - for stdout
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)
    
    # File handler with rotation (10MB per file, keep 5 backups)
    file_handler = RotatingFileHandler(
        Config.LOG_FILE,
        maxBytes=Config.LOG_MAX_BYTES,
        backupCount=Config.LOG_BACKUP_COUNT,
        encoding='utf-8'
    )
    file_handler.setLevel(getattr(logging, Config.LOG_LEVEL))
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    
    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    # Prevent propagation to root logger
    logger.propagate = False
    
    logger.info("=" * 80)
    logger.info("RCA Analyzer Bot - Logging initialized successfully")
    logger.info(f"Log Level: {Config.LOG_LEVEL}")
    logger.info(f"Log File: {Config.LOG_FILE}")
    logger.info("=" * 80)
    
    return logger

def get_logger(name: str):
    """
    Get a logger instance for a specific module
    
    Args:
        name: Module name (usually __name__)
    
    Returns:
        Logger instance
    """
    return logging.getLogger(f"rca_bot.{name}")