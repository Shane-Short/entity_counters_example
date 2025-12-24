"""
Silver Layer - State Hours Calculation
=======================================
Calculates daily state hours (Running, Idle, Down, Bagged) from EntityStates data.

Features:
- Classifies entity states into Running/Idle/Down categories
- Identifies bagged tools
- Aggregates hours by entity and date
- Parses DAY_SHIFT to extract date
- Duplicate prevention
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List
from datetime import datetime
import re

from utils.logger import StateLogger

logger = logging.getLogger(__name__)


class StateHoursCalculator:
    """
    Calculates state hours from EntityStates data.
    """
    
    def __init__(self, config: Dict):
        """
        Initialize state hours calculator.
        
        Parameters
        ----------
        config : dict
            Configuration dictionary
        """
        self.config = config
        self.state_config = config['entity_states']
        self.running_states = self.state_config['running_states']
        self.idle_states = self.state_config['idle_states']
        self.bagged_state = self.state_config['bagged_state']
        
        # Initialize specialized logger
        self.state_logger = StateLogger(logger, config['logging'])
        
        logger.info("State Hours Calculator initialized")
        logger.info(f"Running states: {self.running_states}")
        logger.info(f"Idle states: {self.idle_states}")
        logger.info(f"Bagged state: {self.bagged_state}")
    
    def parse_day_shift_to_date(self, day_shift: str) -> Optional[datetime.date]:
        """
        Parse DAY_SHIFT column to extract date.
        
        Expected formats:
        - MM/DD-shift
        - MM/DD/YY-shift
        - MM/DD/YYYY-shift
        
        Parameters
        ----------
        day_shift : str
            DAY_SHIFT value
        
        Returns
        -------
        datetime.date or None
            Parsed date
        """
        if pd.isna(day_shift):
            return None
        
        try:
            # Extract date part (before dash)
            date_part = day_shift.split('-')[0]
            
            # Try different date formats
            for fmt in ['%m/%d/%Y', '%m/%d/%y', '%m/%d']:
                try:
                    parsed = datetime.strptime(date_part, fmt)
                    # If year not in format, assume current year
                    if fmt == '%m/%d':
                        current_year = datetime.now().year
                        parsed = parsed.replace(year=current_year)
                    return parsed.date()
                except ValueError:
                    continue
            
            logger.warning(f"Could not parse DAY_SHIFT: {day_shift}")
            return None
            
        except Exception as e:
            logger.error(f"Error parsing DAY_SHIFT '{day_shift}': {e}")
            return None
    
    def classify_state(self, entity_state: str, entity: str, date: str) -> str:
        """
        Classify entity state into Running/Idle/Down/Bagged category.
        
        Parameters
        ----------
        entity_state : str
            State value
        entity : str
            Entity name
        date : str
            Date
        
        Returns
        -------
        str
            'Running', 'Idle', 'Down', or 'Bagged'
        """
        if pd.isna(entity_state):
            return 'Down'
        
        state = str(entity_state).strip()
        
        # Check for bagged
        if state == self.bagged_state:
            self.state_logger.log_bagged_tool(entity, date)
            return 'Bagged'
        
        # Check for running states
        if state in self.running_states:
            return 'Running'
        
        # Check for idle states
        if state in self.idle_states:
            return 'Idle'
        
        # Everything else is down
        self.state_logger.log_unknown_state(entity, state, date)
        return 'Down'
    
    def calculate_daily_state_hours(self, entity_states_df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate daily state hours for each entity.
        
        Parameters
        ----------
        entity_states_df : pd.DataFrame
            EntityStates Bronze data
        
        Returns
        -------
        pd.DataFrame
            Daily state hours by entity
        """
        logger.info("Starting state hours calculation")
        
        # Parse DAY_SHIFT to get date
        entity_states_df = entity_states_df.copy()
        entity_states_df['state_date'] = entity_states_df['DAY_SHIFT'].apply(self.parse_day_shift_to_date)
        
        # Filter out rows where date could not be parsed
        before_filter = len(entity_states_df)
        entity_states_df = entity_states_df[entity_states_df['state_date'].notna()].copy()
        after_filter = len(entity_states_df)
        
        if before_filter > after_filter:
            logger.warning(f"Filtered out {before_filter - after_filter} rows with unparseable dates")
        
        # Classify states
        entity_states_df['state_category'] = entity_states_df.apply(
            lambda row: self.classify_state(row['ENTITY_STATE'], row['ENTITY'], str(row['state_date'])),
            axis=1
        )
        
        # Aggregate hours by entity, date, and state category
        state_hours = entity_states_df.groupby(
            ['ENTITY', 'FAB', 'FAB_ENTITY', 'state_date', 'state_category']
        )['HOURS_IN_STATE'].sum().reset_index()
        
        # Pivot to get columns for each state category
        state_hours_pivot = state_hours.pivot_table(
            index=['ENTITY', 'FAB', 'FAB_ENTITY', 'state_date'],
            columns='state_category',
            values='HOURS_IN_STATE',
            fill_value=0
        ).reset_index()
        
        # Ensure all state columns exist
        for state in ['Running', 'Idle', 'Down', 'Bagged']:
            if state not in state_hours_pivot.columns:
                state_hours_pivot[state] = 0
        
        # Rename columns to lowercase with underscore
        state_hours_pivot = state_hours_pivot.rename(columns={
            'Running': 'running_hours',
            'Idle': 'idle_hours',
            'Down': 'down_hours',
            'Bagged': 'bagged_hours'
        })
        
        # Calculate total hours and bagged flag
        state_hours_pivot['total_hours'] = (
            state_hours_pivot['running_hours'] + 
            state_hours_pivot['idle_hours'] + 
            state_hours_pivot['down_hours'] +
            state_hours_pivot['bagged_hours']
        )
        
        state_hours_pivot['is_bagged'] = state_hours_pivot['bagged_hours'] > 0
        
        # Log state classifications
        for _, row in state_hours_pivot.iterrows():
            self.state_logger.log_state_classification(
                row['ENTITY'], 
                str(row['state_date']), 
                row['running_hours'], 
                row['idle_hours'], 
                row['down_hours']
            )
        
        # Remove duplicates based on ENTITY and state_date
        before_dedup = len(state_hours_pivot)
        state_hours_pivot = state_hours_pivot.drop_duplicates(subset=['ENTITY', 'state_date'], keep='last')
        after_dedup = len(state_hours_pivot)
        
        if before_dedup > after_dedup:
            logger.info(f"Removed {before_dedup - after_dedup} duplicate rows")
        
        logger.info(f"State hours calculation complete: {len(state_hours_pivot)} entity-days")
        logger.info(f"Bagged tools: {state_hours_pivot['is_bagged'].sum()}")
        
        return state_hours_pivot


def calculate_state_hours(config: Dict, entity_states_df: pd.DataFrame) -> pd.DataFrame:
    """
    Standalone function to calculate state hours.
    
    Parameters
    ----------
    config : dict
        Configuration dictionary
    entity_states_df : pd.DataFrame
        EntityStates Bronze data
    
    Returns
    -------
    pd.DataFrame
        Daily state hours by entity
    """
    calculator = StateHoursCalculator(config)
    return calculator.calculate_daily_state_hours(entity_states_df)
