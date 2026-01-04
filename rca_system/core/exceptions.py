class RCAPipelineException(Exception):
    """Base exception for RCA pipeline"""
    pass


class MetricsCalculationError(RCAPipelineException):
    """Error during metrics calculation"""
    pass


class RCAAnalysisError(RCAPipelineException):
    """Error during RCA analysis"""
    pass


class EmailNotificationError(RCAPipelineException):
    """Error sending email notification"""
    pass


class DataValidationError(RCAPipelineException):
    """Data validation error"""
    pass
