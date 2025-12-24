"""
Silver Layer - Enrichment Orchestration
========================================
Orchestrates all Silver layer calculations:
- State hours calculation
- Wafer production calculation
- Part replacement tracking

Combines Bronze data into enriched Silver tables.
"""

import pandas as pd
import logging
from typing import Dict, Tuple

from etl.silver.state_hours import calculate_state_hours
from etl.silver.wafer_production import calculate_wafer_production
from etl.silver.part_replacements import track_part_replacements

logger = logging.getLogger(__name__)


class SilverEnrichment:
    """
    Orchestrates Silver layer enrichment process.
    """
    
    def __init__(self, config: Dict):
        """
        Initialize Silver enrichment.
        
        Parameters
        ----------
        config : dict
            Configuration dictionary
        """
        self.config = config
        logger.info("Silver Enrichment initialized")
    
    def enrich(self, entity_states_df: pd.DataFrame, counters_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Run all Silver layer enrichment calculations.
        
        Parameters
        ----------
        entity_states_df : pd.DataFrame
            EntityStates Bronze data
        counters_df : pd.DataFrame
            Counters Bronze data
        
        Returns
        -------
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]
            (state_hours, wafer_production, part_replacements)
        """
        logger.info("Starting Silver layer enrichment")
        
        # Step 1: Calculate state hours
        logger.info("STEP 1: Calculating state hours")
        state_hours_df = calculate_state_hours(self.config, entity_states_df)
        logger.info(f"State hours complete: {len(state_hours_df)} rows")
        
        # Step 2: Calculate wafer production (requires state hours)
        logger.info("STEP 2: Calculating wafer production")
        production_df = calculate_wafer_production(self.config, counters_df, state_hours_df)
        logger.info(f"Wafer production complete: {len(production_df)} rows")
        
        # Step 3: Track part replacements
        logger.info("STEP 3: Tracking part replacements")
        replacements_df = track_part_replacements(self.config, production_df)
        logger.info(f"Part replacements complete: {len(replacements_df)} rows")
        
        logger.info("Silver layer enrichment complete")
        
        return state_hours_df, production_df, replacements_df


def run_silver_enrichment(config: Dict, 
                         entity_states_df: pd.DataFrame, 
                         counters_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Standalone function to run Silver enrichment.
    
    Parameters
    ----------
    config : dict
        Configuration dictionary
    entity_states_df : pd.DataFrame
        EntityStates Bronze data
    counters_df : pd.DataFrame
        Counters Bronze data
    
    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]
        (state_hours, wafer_production, part_replacements)
    """
    enrichment = SilverEnrichment(config)
    return enrichment.enrich(entity_states_df, counters_df)
