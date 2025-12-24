"""
Logging Configuration for Entity States & Counters Pipeline
============================================================
Provides comprehensive audit trail logging for:
- File discovery and loading
- Wafer production calculations
- Part replacement detection
- Entity state classification
- Data quality issues
"""

import logging
import sys
from pathlib import Path
from datetime import datetime


def setup_logger(name: str = __name__, level: str = 'INFO', log_file: str = None) -> logging.Logger:
    """
    Set up logger with console and optional file handlers.
    
    Parameters
    ----------
    name : str
        Logger name
    level : str
        Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    log_file : str, optional
        Path to log file (creates logs directory if needed)
    
    Returns
    -------
    logging.Logger
        Configured logger
    """
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    # Remove existing handlers
    logger.handlers = []
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        fmt='%(asctime)s | %(name)s | %(levelname)s | %(funcName)s:%(lineno)d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    simple_formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)s | %(message)s',
        datefmt='%H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(simple_formatter)
    logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_path, mode='a', encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(detailed_formatter)
        logger.addHandler(file_handler)
        
        logger.info(f"Logging to file: {log_path}")
    
    return logger


class WaferProductionLogger:
    """
    Specialized logger for wafer production calculation audit trail.
    """
    
    def __init__(self, logger: logging.Logger, config: dict):
        self.logger = logger
        self.config = config.get('wafer_production_logging', {})
    
    def log_counter_search(self, entity: str, date: str, keywords_tried: list):
        """Log counter keyword search process."""
        if self.config.get('log_counter_used', True):
            self.logger.debug(f"Entity {entity} ({date}): Searching for counters with keywords: {keywords_tried}")
    
    def log_counter_found(self, entity: str, date: str, column_name: str, value: float, keyword: str):
        """Log when counter is found."""
        if self.config.get('log_counter_used', True):
            self.logger.info(f"Entity {entity} ({date}): Using counter '{column_name}' (keyword: '{keyword}', value: {value})")
    
    def log_no_counter_found(self, entity: str, date: str, keywords_tried: list):
        """Log when no counter is found."""
        if self.config.get('log_no_counter_found', True):
            self.logger.warning(f"Entity {entity} ({date}): No counter found with keywords: {keywords_tried}")
    
    def log_negative_change(self, entity: str, date: str, counter: str, prev_value: float, curr_value: float, change: float):
        """Log negative counter changes."""
        if self.config.get('log_negative_changes', True):
            self.logger.warning(
                f"Entity {entity} ({date}): Negative change in {counter}: "
                f"{prev_value} -> {curr_value} (change: {change})"
            )
    
    def log_part_replacement(self, entity: str, date: str, counter: str, last_value: float, new_value: float, threshold: float):
        """Log detected part replacement."""
        if self.config.get('log_replacements', True):
            self.logger.info(
                f"PART REPLACEMENT DETECTED - Entity {entity} ({date}): "
                f"{counter} dropped from {last_value} to {new_value} "
                f"(threshold: {threshold})"
            )
    
    def log_fallback_used(self, entity: str, date: str, primary_keyword: str, fallback_keyword: str, reason: str):
        """Log when fallback counter is used."""
        self.logger.info(
            f"Entity {entity} ({date}): Fallback counter used - "
            f"'{primary_keyword}' failed ({reason}), using '{fallback_keyword}'"
        )
    
    def log_wafer_calculation(self, entity: str, date: str, counter_change: float, running_hours: float, wafers_per_hour: float):
        """Log final wafer calculation."""
        self.logger.debug(
            f"Entity {entity} ({date}): Wafer calculation - "
            f"Counter change: {counter_change}, Running hours: {running_hours}, "
            f"Wafers/hr: {wafers_per_hour:.2f}"
        )


class StateLogger:
    """
    Specialized logger for entity state classification.
    """
    
    def __init__(self, logger: logging.Logger, config: dict):
        self.logger = logger
        self.config = config.get('state_logging', {})
    
    def log_unknown_state(self, entity: str, state: str, date: str):
        """Log unknown entity states."""
        if self.config.get('log_unknown_states', True):
            self.logger.warning(f"Entity {entity} ({date}): Unknown state '{state}' encountered")
    
    def log_bagged_tool(self, entity: str, date: str):
        """Log when tool is marked as bagged."""
        if self.config.get('log_bagged_tools', True):
            self.logger.info(f"Entity {entity} ({date}): Tool marked as BAGGED")
    
    def log_state_classification(self, entity: str, date: str, running_hrs: float, idle_hrs: float, down_hrs: float):
        """Log state hour classification."""
        self.logger.debug(
            f"Entity {entity} ({date}): State hours - "
            f"Running: {running_hrs:.2f}, Idle: {idle_hrs:.2f}, Down: {down_hrs:.2f}"
        )


def create_run_log_file(base_dir: str = 'logs') -> str:
    """
    Create timestamped log file name for pipeline run.
    
    Parameters
    ----------
    base_dir : str
        Base directory for logs
    
    Returns
    -------
    str
        Full path to log file
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = Path(base_dir) / f'entity_counters_pipeline_{timestamp}.log'
    return str(log_file)
