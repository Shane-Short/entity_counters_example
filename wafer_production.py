"""
Silver Layer - Wafer Production Calculation
============================================
Calculates daily wafer production using part counter changes.

Features:
- Counter keyword search (Focus -> APCCounter -> ESCCounter -> PMACounter)
- Part replacement detection (negative threshold: -1000)
- Fallback logic when primary counter fails
- Wafers per running hour calculation
- Comprehensive logging of all decisions
- Duplicate prevention
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime

from utils.logger import WaferProductionLogger

logger = logging.getLogger(__name__)


class WaferProductionCalculator:
    """
    Calculates wafer production metrics from part counter data.
    """
    
    def __init__(self, config: Dict):
        """
        Initialize wafer production calculator.
        
        Parameters
        ----------
        config : dict
            Configuration dictionary
        """
        self.config = config
        self.wafer_config = config['wafer_production']
        self.primary_keywords = self.wafer_config['primary_keywords']
        self.fallback_keywords = self.wafer_config['fallback_keywords']
        self.replacement_threshold = self.wafer_config['part_replacement']['negative_threshold']
        
        # Initialize specialized logger
        self.prod_logger = WaferProductionLogger(logger, config['logging'])
        
        logger.info("Wafer Production Calculator initialized")
        logger.info(f"Primary keywords: {self.primary_keywords}")
        logger.info(f"Fallback keywords: {self.fallback_keywords}")
        logger.info(f"Replacement threshold: {self.replacement_threshold}")
    
    def find_counter_column(self, row: pd.Series, keywords: List[str]) -> Optional[Tuple[str, float, str]]:
        """
        Find first counter column that matches keywords and has a value.
        
        Parameters
        ----------
        row : pd.Series
            DataFrame row
        keywords : List[str]
            List of keywords to search for
        
        Returns
        -------
        Tuple[str, float, str] or None
            (column_name, value, keyword_used) if found, None otherwise
        """
        for keyword in keywords:
            # Find columns containing this keyword
            matching_cols = [col for col in row.index if keyword in col and col.endswith('Counter')]
            
            # Check each matching column for a non-null value
            for col in matching_cols:
                value = row[col]
                if pd.notna(value) and value > 0:
                    return (col, value, keyword)
        
        return None
    
    def calculate_counter_change(self, 
                                current_row: pd.Series, 
                                previous_row: Optional[pd.Series],
                                counter_col: str) -> Optional[float]:
        """
        Calculate change in counter value from previous day.
        
        Parameters
        ----------
        current_row : pd.Series
            Current day row
        previous_row : pd.Series or None
            Previous day row
        counter_col : str
            Counter column name
        
        Returns
        -------
        float or None
            Counter change, or None if cannot calculate
        """
        if previous_row is None:
            return None
        
        current_val = current_row[counter_col]
        previous_val = previous_row.get(counter_col, np.nan)
        
        if pd.isna(current_val) or pd.isna(previous_val):
            return None
        
        change = current_val - previous_val
        return change
    
    def detect_part_replacement(self, change: float, entity: str, date: str, 
                               counter: str, current_val: float, 
                               previous_val: float) -> bool:
        """
        Detect if change indicates a part replacement.
        
        Parameters
        ----------
        change : float
            Counter change value
        entity : str
            Entity name
        date : str
            Date
        counter : str
            Counter column name
        current_val : float
            Current counter value
        previous_val : float
            Previous counter value
        
        Returns
        -------
        bool
            True if replacement detected
        """
        if change < self.replacement_threshold:
            self.prod_logger.log_part_replacement(
                entity, date, counter, previous_val, current_val, self.replacement_threshold
            )
            return True
        return False
    
    def calculate_wafer_production_single_row(self, 
                                             current_row: pd.Series,
                                             previous_row: Optional[pd.Series],
                                             running_hours: float) -> Dict:
        """
        Calculate wafer production for a single entity-date row.
        
        Parameters
        ----------
        current_row : pd.Series
            Current day row with counter data
        previous_row : pd.Series or None
            Previous day row
        running_hours : float
            Running state hours for this day
        
        Returns
        -------
        Dict
            Dictionary with production metrics
        """
        entity = current_row['ENTITY']
        date = str(current_row['counter_date'])
        
        result = {
            'ENTITY': entity,
            'counter_date': current_row['counter_date'],
            'counter_column_used': None,
            'counter_keyword_used': None,
            'counter_current_value': None,
            'counter_previous_value': None,
            'counter_change': None,
            'part_replacement_detected': False,
            'wafers_produced': None,
            'running_hours': running_hours,
            'wafers_per_hour': None,
            'calculation_notes': []
        }
        
        # Try primary keywords first
        all_keywords = self.primary_keywords + self.fallback_keywords
        counter_found = self.find_counter_column(current_row, all_keywords)
        
        if not counter_found:
            self.prod_logger.log_no_counter_found(entity, date, all_keywords)
            result['calculation_notes'].append('No counter found with any keyword')
            return result
        
        counter_col, current_val, keyword = counter_found
        result['counter_column_used'] = counter_col
        result['counter_keyword_used'] = keyword
        result['counter_current_value'] = current_val
        
        self.prod_logger.log_counter_found(entity, date, counter_col, current_val, keyword)
        
        # Calculate change from previous day
        if previous_row is None:
            result['calculation_notes'].append('First day - no previous value')
            return result
        
        # Get previous value for same counter
        if counter_col not in previous_row.index:
            result['calculation_notes'].append(f'Counter {counter_col} not in previous row')
            return result
        
        previous_val = previous_row[counter_col]
        if pd.isna(previous_val):
            result['calculation_notes'].append('Previous value is null')
            return result
        
        result['counter_previous_value'] = previous_val
        change = current_val - previous_val
        result['counter_change'] = change
        
        # Check for negative change (part replacement)
        if change < 0:
            self.prod_logger.log_negative_change(entity, date, counter_col, previous_val, current_val, change)
            
            # Check if it's a part replacement
            if self.detect_part_replacement(change, entity, date, counter_col, current_val, previous_val):
                result['part_replacement_detected'] = True
                result['calculation_notes'].append(f'Part replacement: {counter_col} reset from {previous_val} to {current_val}')
                
                # Try fallback counter
                if keyword in self.primary_keywords:
                    fallback_found = self.find_counter_column(current_row, self.fallback_keywords)
                    if fallback_found:
                        fallback_col, fallback_val, fallback_keyword = fallback_found
                        self.prod_logger.log_fallback_used(entity, date, keyword, fallback_keyword, 'part replacement')
                        
                        # Recalculate with fallback
                        if fallback_col in previous_row.index:
                            prev_fallback = previous_row[fallback_col]
                            if pd.notna(prev_fallback):
                                change = fallback_val - prev_fallback
                                if change >= 0:
                                    result['counter_column_used'] = fallback_col
                                    result['counter_keyword_used'] = fallback_keyword
                                    result['counter_current_value'] = fallback_val
                                    result['counter_previous_value'] = prev_fallback
                                    result['counter_change'] = change
                                    result['calculation_notes'].append(f'Used fallback counter: {fallback_col}')
                
                # If still negative, set to zero
                if result['counter_change'] < 0:
                    result['counter_change'] = 0
                    result['calculation_notes'].append('Counter change set to 0 (part replacement)')
        
        # Calculate wafers produced and wafers per hour
        if result['counter_change'] is not None and result['counter_change'] >= 0:
            result['wafers_produced'] = result['counter_change']
            
            if running_hours > 0:
                result['wafers_per_hour'] = result['wafers_produced'] / running_hours
                self.prod_logger.log_wafer_calculation(
                    entity, date, result['counter_change'], running_hours, result['wafers_per_hour']
                )
            else:
                result['calculation_notes'].append('No running hours - cannot calculate wafers/hour')
        
        return result
    
    def calculate_for_dataframe(self, counters_df: pd.DataFrame, state_hours_df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate wafer production for entire DataFrame.
        
        Parameters
        ----------
        counters_df : pd.DataFrame
            Counters data (from Bronze)
        state_hours_df : pd.DataFrame
            State hours data (running hours by entity-date)
        
        Returns
        -------
        pd.DataFrame
            Daily production metrics
        """
        logger.info("Starting wafer production calculation")
        
        # Sort by entity and date
        counters_df = counters_df.sort_values(['ENTITY', 'counter_date']).reset_index(drop=True)
        
        results = []
        
        # Group by entity
        for entity, entity_group in counters_df.groupby('ENTITY'):
            entity_group = entity_group.sort_values('counter_date').reset_index(drop=True)
            
            # Process each day
            for idx, current_row in entity_group.iterrows():
                # Get previous row
                previous_row = entity_group.iloc[idx - 1] if idx > 0 else None
                
                # Get running hours for this day
                date = current_row['counter_date']
                running_hours_row = state_hours_df[
                    (state_hours_df['ENTITY'] == entity) & 
                    (state_hours_df['state_date'] == date)
                ]
                
                running_hours = running_hours_row['running_hours'].values[0] if len(running_hours_row) > 0 else 0
                
                # Calculate production
                result = self.calculate_wafer_production_single_row(current_row, previous_row, running_hours)
                results.append(result)
        
        # Convert to DataFrame
        production_df = pd.DataFrame(results)
        
        # Convert notes list to string
        production_df['calculation_notes'] = production_df['calculation_notes'].apply(lambda x: '; '.join(x) if x else None)
        
        # Remove duplicates based on ENTITY and counter_date
        before_dedup = len(production_df)
        production_df = production_df.drop_duplicates(subset=['ENTITY', 'counter_date'], keep='last')
        after_dedup = len(production_df)
        
        if before_dedup > after_dedup:
            logger.info(f"Removed {before_dedup - after_dedup} duplicate rows")
        
        logger.info(f"Wafer production calculation complete: {len(production_df)} rows")
        logger.info(f"Rows with wafers calculated: {production_df['wafers_produced'].notna().sum()}")
        logger.info(f"Part replacements detected: {production_df['part_replacement_detected'].sum()}")
        
        return production_df


def calculate_wafer_production(config: Dict, counters_df: pd.DataFrame, state_hours_df: pd.DataFrame) -> pd.DataFrame:
    """
    Standalone function to calculate wafer production.
    
    Parameters
    ----------
    config : dict
        Configuration dictionary
    counters_df : pd.DataFrame
        Counters Bronze data
    state_hours_df : pd.DataFrame
        State hours data with running_hours column
    
    Returns
    -------
    pd.DataFrame
        Daily production metrics
    """
    calculator = WaferProductionCalculator(config)
    return calculator.calculate_for_dataframe(counters_df, state_hours_df)
