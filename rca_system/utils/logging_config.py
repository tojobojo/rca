import logging
import sys
from pathlib import Path
from config.settings import Config


def setup_logging(name: str = "rca_pipeline") -> logging.Logger:
	"""
	Setup production-grade logging with file and console handlers
    
	Args:
		name: Logger name
        
	Returns:
		Configured logger instance
	"""
	logger = logging.getLogger(name)
	logger.setLevel(getattr(logging, Config.logging.LOG_LEVEL))
    
	# Avoid duplicate handlers
	if logger.handlers:
		return logger
    
	formatter = logging.Formatter(Config.logging.LOG_FORMAT)
    
	# Console handler
	console_handler = logging.StreamHandler(sys.stdout)
	console_handler.setLevel(logging.INFO)
	console_handler.setFormatter(formatter)
	logger.addHandler(console_handler)
    
	# File handler (if path provided)
	if Config.logging.LOG_FILE:
		try:
			log_path = Path(Config.logging.LOG_FILE)
			log_path.parent.mkdir(parents=True, exist_ok=True)
            
			file_handler = logging.FileHandler(Config.logging.LOG_FILE)
			file_handler.setLevel(logging.DEBUG)
			file_handler.setFormatter(formatter)
			logger.addHandler(file_handler)
		except Exception as e:
			logger.warning(f"Could not setup file logging: {e}")
    
	return logger
