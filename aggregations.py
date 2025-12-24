"""
Gold Layer - Aggregations
==========================
Creates aggregated KPI tables for Power BI consumption.

Tables created:
- fact_daily_production: Daily production metrics by entity
- fact_weekly_production: Weekly production metrics by entity  
- fact_state_hours_daily: Daily state hours by entity
- fact_state_hours_weekly: Weekly state hours by entity
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


class GoldAggregations:
    """
    Creates Gold layer aggregation tables.
    """
    
    def __init__(self, config: Dict):
        """
        Initialize Gold aggregations.
        
        Parameters
        ----------
        config : dict
            Configuration dictionary
        """
        self.config = config
        logger.info("Gold Aggregations initialized")
    
    def create_daily_production_fact(self, production_df: pd.DataFrame, state_hours_df: pd.DataFrame) -> pd.DataFrame:
        """
        Create daily production fact table.
        
        Parameters
        ----------
        production_df : pd.DataFrame
            Wafer production Silver data
        state_hours_df : pd.DataFrame
            State hours Silver data
        
        Returns
        -------
        pd.DataFrame
            Daily production fact table
        """
        logger.info("Creating daily production fact table")
        
        # Merge production with state hours
        daily_fact = production_df.merge(
            state_hours_df[['ENTITY', 'state_date', 'FAB', 'FAB_ENTITY', 
                           'running_hours', 'idle_hours', 'down_hours', 
                           'bagged_hours', 'total_hours', 'is_bagged']],
            left_on=['ENTITY', 'counter_date'],
            right_on=['ENTITY', 'state_date'],
            how='left'
        )
        
        # Select and rename columns
        daily_fact = daily_fact[[
            'ENTITY', 'FAB', 'FAB_ENTITY', 'counter_date',
            'wafers_produced', 'wafers_per_hour',
            'running_hours', 'idle_hours', 'down_hours', 'bagged_hours', 'total_hours',
            'is_bagged', 'part_replacement_detected',
            'counter_column_used', 'counter_keyword_used'
        ]].copy()
        
        # Rename for clarity
        daily_fact = daily_fact.rename(columns={
            'counter_date': 'production_date'
        })
        
        # Add calculation timestamp
        daily_fact['calculation_timestamp'] = datetime.now().isoformat()
        
        # Remove duplicates
        before_dedup = len(daily_fact)
        daily_fact = daily_fact.drop_duplicates(
            subset=['ENTITY', 'production_date'],
            keep='last'
        )
        after_dedup = len(daily_fact)
        
        if before_dedup > after_dedup:
            logger.info(f"Removed {before_dedup - after_dedup} duplicate rows from daily production fact")
        
        logger.info(f"Daily production fact table created: {len(daily_fact)} rows")
        
        return daily_fact
    
    def create_weekly_production_fact(self, daily_fact: pd.DataFrame) -> pd.DataFrame:
        """
        Create weekly production fact table.
        
        Parameters
        ----------
        daily_fact : pd.DataFrame
            Daily production fact table
        
        Returns
        -------
        pd.DataFrame
            Weekly production fact table
        """
        logger.info("Creating weekly production fact table")
        
        # Add work week column (need to parse from date)
        daily_fact['production_date_dt'] = pd.to_datetime(daily_fact['production_date'])
        daily_fact['year'] = daily_fact['production_date_dt'].dt.year
        daily_fact['week'] = daily_fact['production_date_dt'].dt.isocalendar().week
        daily_fact['YEARWW'] = daily_fact['year'].astype(str) + 'WW' + daily_fact['week'].astype(str).str.zfill(2)
        
        # Aggregate by entity and week
        weekly_fact = daily_fact.groupby(['ENTITY', 'FAB', 'FAB_ENTITY', 'YEARWW']).agg({
            'wafers_produced': 'sum',
            'running_hours': 'sum',
            'idle_hours': 'sum',
            'down_hours': 'sum',
            'bagged_hours': 'sum',
            'total_hours': 'sum',
            'part_replacement_detected': 'sum',
            'production_date': ['min', 'max', 'count']
        }).reset_index()
        
        # Flatten column names
        weekly_fact.columns = [
            'ENTITY', 'FAB', 'FAB_ENTITY', 'YEARWW',
            'total_wafers_produced', 'total_running_hours', 'total_idle_hours',
            'total_down_hours', 'total_bagged_hours', 'total_hours',
            'part_replacements_count', 'week_start_date', 'week_end_date', 'days_with_data'
        ]
        
        # Calculate weekly wafers per hour
        weekly_fact['avg_wafers_per_hour'] = np.where(
            weekly_fact['total_running_hours'] > 0,
            weekly_fact['total_wafers_produced'] / weekly_fact['total_running_hours'],
            np.nan
        )
        
        # Add calculation timestamp
        weekly_fact['calculation_timestamp'] = datetime.now().isoformat()
        
        # Remove duplicates
        before_dedup = len(weekly_fact)
        weekly_fact = weekly_fact.drop_duplicates(
            subset=['ENTITY', 'YEARWW'],
            keep='last'
        )
        after_dedup = len(weekly_fact)
        
        if before_dedup > after_dedup:
            logger.info(f"Removed {before_dedup - after_dedup} duplicate rows from weekly production fact")
        
        logger.info(f"Weekly production fact table created: {len(weekly_fact)} rows")
        
        return weekly_fact
    
    def create_state_hours_daily_fact(self, state_hours_df: pd.DataFrame) -> pd.DataFrame:
        """
        Create daily state hours fact table.
        
        Parameters
        ----------
        state_hours_df : pd.DataFrame
            State hours Silver data
        
        Returns
        -------
        pd.DataFrame
            Daily state hours fact table
        """
        logger.info("Creating daily state hours fact table")
        
        # Copy and add calculated metrics
        daily_state_fact = state_hours_df.copy()
        
        # Calculate utilization percentages
        daily_state_fact['running_pct'] = np.where(
            daily_state_fact['total_hours'] > 0,
            (daily_state_fact['running_hours'] / daily_state_fact['total_hours']) * 100,
            0
        )
        
        daily_state_fact['idle_pct'] = np.where(
            daily_state_fact['total_hours'] > 0,
            (daily_state_fact['idle_hours'] / daily_state_fact['total_hours']) * 100,
            0
        )
        
        daily_state_fact['down_pct'] = np.where(
            daily_state_fact['total_hours'] > 0,
            (daily_state_fact['down_hours'] / daily_state_fact['total_hours']) * 100,
            0
        )
        
        # Add calculation timestamp
        daily_state_fact['calculation_timestamp'] = datetime.now().isoformat()
        
        # Already deduplicated in state_hours calculation, but verify
        before_dedup = len(daily_state_fact)
        daily_state_fact = daily_state_fact.drop_duplicates(
            subset=['ENTITY', 'state_date'],
            keep='last'
        )
        after_dedup = len(daily_state_fact)
        
        if before_dedup > after_dedup:
            logger.info(f"Removed {before_dedup - after_dedup} duplicate rows from daily state hours fact")
        
        logger.info(f"Daily state hours fact table created: {len(daily_state_fact)} rows")
        
        return daily_state_fact
    
    def create_state_hours_weekly_fact(self, daily_state_fact: pd.DataFrame) -> pd.DataFrame:
        """
        Create weekly state hours fact table.
        
        Parameters
        ----------
        daily_state_fact : pd.DataFrame
            Daily state hours fact table
        
        Returns
        -------
        pd.DataFrame
            Weekly state hours fact table
        """
        logger.info("Creating weekly state hours fact table")
        
        # Add work week column
        daily_state_fact['state_date_dt'] = pd.to_datetime(daily_state_fact['state_date'])
        daily_state_fact['year'] = daily_state_fact['state_date_dt'].dt.year
        daily_state_fact['week'] = daily_state_fact['state_date_dt'].dt.isocalendar().week
        daily_state_fact['YEARWW'] = daily_state_fact['year'].astype(str) + 'WW' + daily_state_fact['week'].astype(str).str.zfill(2)
        
        # Aggregate by entity and week
        weekly_state_fact = daily_state_fact.groupby(['ENTITY', 'FAB', 'FAB_ENTITY', 'YEARWW']).agg({
            'running_hours': 'sum',
            'idle_hours': 'sum',
            'down_hours': 'sum',
            'bagged_hours': 'sum',
            'total_hours': 'sum',
            'is_bagged': 'max',  # True if bagged any day in the week
            'state_date': ['min', 'max', 'count']
        }).reset_index()
        
        # Flatten column names
        weekly_state_fact.columns = [
            'ENTITY', 'FAB', 'FAB_ENTITY', 'YEARWW',
            'total_running_hours', 'total_idle_hours', 'total_down_hours',
            'total_bagged_hours', 'total_hours', 'was_bagged_any_day',
            'week_start_date', 'week_end_date', 'days_with_data'
        ]
        
        # Calculate weekly utilization percentages
        weekly_state_fact['running_pct'] = np.where(
            weekly_state_fact['total_hours'] > 0,
            (weekly_state_fact['total_running_hours'] / weekly_state_fact['total_hours']) * 100,
            0
        )
        
        weekly_state_fact['idle_pct'] = np.where(
            weekly_state_fact['total_hours'] > 0,
            (weekly_state_fact['total_idle_hours'] / weekly_state_fact['total_hours']) * 100,
            0
        )
        
        weekly_state_fact['down_pct'] = np.where(
            weekly_state_fact['total_hours'] > 0,
            (weekly_state_fact['total_down_hours'] / weekly_state_fact['total_hours']) * 100,
            0
        )
        
        # Add calculation timestamp
        weekly_state_fact['calculation_timestamp'] = datetime.now().isoformat()
        
        # Remove duplicates
        before_dedup = len(weekly_state_fact)
        weekly_state_fact = weekly_state_fact.drop_duplicates(
            subset=['ENTITY', 'YEARWW'],
            keep='last'
        )
        after_dedup = len(weekly_state_fact)
        
        if before_dedup > after_dedup:
            logger.info(f"Removed {before_dedup - after_dedup} duplicate rows from weekly state hours fact")
        
        logger.info(f"Weekly state hours fact table created: {len(weekly_state_fact)} rows")
        
        return weekly_state_fact
    
    def create_all_facts(self, 
                        production_df: pd.DataFrame, 
                        state_hours_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Create all Gold layer fact tables.
        
        Parameters
        ----------
        production_df : pd.DataFrame
            Wafer production Silver data
        state_hours_df : pd.DataFrame
            State hours Silver data
        
        Returns
        -------
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]
            (daily_production, weekly_production, daily_state_hours, weekly_state_hours)
        """
        logger.info("Creating all Gold layer fact tables")
        
        # Create daily tables
        daily_production = self.create_daily_production_fact(production_df, state_hours_df)
        daily_state_hours = self.create_state_hours_daily_fact(state_hours_df)
        
        # Create weekly tables
        weekly_production = self.create_weekly_production_fact(daily_production)
        weekly_state_hours = self.create_state_hours_weekly_fact(daily_state_hours)
        
        logger.info("All Gold layer fact tables created")
        
        return daily_production, weekly_production, daily_state_hours, weekly_state_hours


def create_gold_facts(config: Dict,
                     production_df: pd.DataFrame,
                     state_hours_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Standalone function to create Gold layer facts.
    
    Parameters
    ----------
    config : dict
        Configuration dictionary
    production_df : pd.DataFrame
        Wafer production Silver data
    state_hours_df : pd.DataFrame
        State hours Silver data
    
    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]
        (daily_production, weekly_production, daily_state_hours, weekly_state_hours)
    """
    aggregator = GoldAggregations(config)
    return aggregator.create_all_facts(production_df, state_hours_df)
