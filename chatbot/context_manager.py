# context_manager.py - Context management for Pipeline RCA Analysis
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import datetime
import logging

logger = logging.getLogger("rca_bot.context")


@dataclass
class PipelineAnalysisContext:
    """
    Context for pipeline RCA analysis runs.
    
    This context is passed to all agent tools via RunContextWrapper,
    enabling stateful conversation and intelligent defaults.
    """
    
    # Core analysis context
    selected_pipeline: Optional[str] = None
    analysis_start_date: Optional[str] = None
    analysis_end_date: Optional[str] = None
    selected_steps: List[str] = field(default_factory=list)
    
    # Fuzzy matching state
    fuzzy_match_candidates: List[Dict[str, Any]] = field(default_factory=list)
    pending_confirmation: Optional[str] = None
    
    # Query history for optimization
    last_query_params: Dict[str, Any] = field(default_factory=dict)
    
    # User session info
    user_id: Optional[str] = None
    session_start: datetime = field(default_factory=datetime.now)
    
    def set_pipeline(self, pipeline_name: str):
        """
        Set the selected pipeline and clear related state.
        
        Args:
            pipeline_name: Name of the pipeline to analyze
        """
        logger.info(f"Context: Setting pipeline to '{pipeline_name}'")
        self.selected_pipeline = pipeline_name
        self.fuzzy_match_candidates = []
        self.pending_confirmation = None
    
    def set_date_range(self, start_date: str, end_date: str):
        """
        Set the analysis date range.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
        """
        logger.info(f"Context: Setting date range to {start_date} - {end_date}")
        self.analysis_start_date = start_date
        self.analysis_end_date = end_date
    
    def add_step(self, step_name: str):
        """
        Add a step to the focus list.
        
        Args:
            step_name: Name of the step to add
        """
        if step_name not in self.selected_steps:
            logger.info(f"Context: Adding step '{step_name}' to focus list")
            self.selected_steps.append(step_name)
    
    def remove_step(self, step_name: str):
        """
        Remove a step from the focus list.
        
        Args:
            step_name: Name of the step to remove
        """
        if step_name in self.selected_steps:
            logger.info(f"Context: Removing step '{step_name}' from focus list")
            self.selected_steps.remove(step_name)
    
    def clear_pipeline_context(self):
        """Clear all pipeline-specific context."""
        logger.info("Context: Clearing all pipeline-specific context")
        self.selected_pipeline = None
        self.selected_steps = []
        self.analysis_start_date = None
        self.analysis_end_date = None
        self.fuzzy_match_candidates = []
        self.pending_confirmation = None
        self.last_query_params = {}
    
    def clear_all(self):
        """Clear all context (full reset)."""
        logger.info("Context: Full reset")
        self.clear_pipeline_context()
        # Keep user_id and session_start
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the current context.
        
        Returns:
            Dictionary with context summary
        """
        return {
            "pipeline": self.selected_pipeline,
            "date_range": {
                "start": self.analysis_start_date,
                "end": self.analysis_end_date
            } if self.analysis_start_date else None,
            "steps": self.selected_steps,
            "has_pending_confirmation": self.pending_confirmation is not None
        }
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        parts = []
        if self.selected_pipeline:
            parts.append(f"pipeline={self.selected_pipeline}")
        if self.analysis_start_date:
            parts.append(f"dates={self.analysis_start_date} to {self.analysis_end_date}")
        if self.selected_steps:
            parts.append(f"steps={len(self.selected_steps)}")
        
        return f"PipelineAnalysisContext({', '.join(parts) if parts else 'empty'})"
