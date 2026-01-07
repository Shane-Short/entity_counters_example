"""
Silver Layer - Part Replacement Tracking
========================================
Tracks all part replacement events detected during wafer production calculation.

Features:
- Extracts replacement events from wafer production results
- Records replacement date, counter, and values
- Tracks part lifecycle metrics
- Deduplication of replacement events
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict
from datetime import datetime

logger = logging.getLogger(__name__)


class PartReplacementTracker:
    """
    Tracks part replacement events from wafer production data.
    """
    
    def __init__(self, config: Dict):
        """
        Initialize part replacement tracker.
        
        Parameters
        ----------
        config : dict
            Configuration dictionary
        """
        self.config = config
        self.replacement_config = config['wafer_production']['part_replacement']
        
        logger.info("Part Replacement Tracker initialized")
    
    def extract_replacements(self, production_df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract part replacement events from production data.
        
        Parameters
        ----------
        production_df : pd.DataFrame
            Wafer production data with part_replacement_detected column
        
        Returns
        -------
        pd.DataFrame
            Part replacement events
        """
        logger.info("Extracting part replacement events")
        
        # Filter to rows where replacement was detected
        replacements = production_df[production_df['part_replacement_detected'] == True].copy()
        
        if len(replacements) == 0:
            logger.info("No part replacements detected")
            return pd.DataFrame()
        
        # Create replacement tracking table with FAB and FAB_ENTITY
        replacement_events = pd.DataFrame({
            'FAB': replacements['FAB'],
            'ENTITY': replacements['ENTITY'],
            'FAB_ENTITY': replacements['FAB_ENTITY'],
            'replacement_date': replacements['counter_date'],
            'part_counter_name': replacements['counter_column_used'],
            'last_value_before_replacement': replacements['counter_previous_value'],
            'first_value_after_replacement': replacements['counter_current_value'],
            'value_drop': replacements['counter_previous_value'] - replacements['counter_current_value'],
            'part_wafers_at_replacement': replacements['counter_previous_value'],
            'notes': replacements['calculation_notes'],
            'replacement_detected_ts': datetime.now()
        })
        
        # Remove duplicates (same FAB_ENTITY + date + part)
        before_dedup = len(replacement_events)
        replacement_events = replacement_events.drop_duplicates(
            subset=['FAB_ENTITY', 'replacement_date', 'part_counter_name'],
            keep='last'
        )
        after_dedup = len(replacement_events)
        
        if before_dedup > after_dedup:
            logger.info(f"Removed {before_dedup - after_dedup} duplicate replacement events")
        
        logger.info(f"Part replacement tracking complete: {len(replacement_events)} replacement events")
        
        return replacement_events


def track_part_replacements(config: Dict, production_df: pd.DataFrame) -> pd.DataFrame:
    """
    Standalone function to track part replacements.
    
    Parameters
    ----------
    config : dict
        Configuration dictionary
    production_df : pd.DataFrame
        Wafer production data
    
    Returns
    -------
    pd.DataFrame
        Part replacement events
    """
    tracker = PartReplacementTracker(config)
    return tracker.extract_replacements(production_df)
