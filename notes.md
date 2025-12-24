"""
Helper Functions for Entity States & Counters Pipeline
======================================================
Contains reusable functions for:
- Work week calculation (Intel fiscal calendar)
- File discovery (EntityStates and latest Counters)
- Entity name normalization (PC -> PM conversion)
- FAB_ENTITY key creation
- DataFrame utilities
"""

from datetime import datetime, timezone, timedelta
from pathlib import Path
import logging
import pandas as pd
import numpy as np
from typing import Tuple, Dict, List, Optional
import os

logger = logging.getLogger(__name__)


# ============================================================================
# Work Week Calculation
# ============================================================================

def get_intel_ww(dt: Optional[datetime] = None) -> str:
    """
    Calculate Intel work week string (fiscal calendar).
    
    Intel's fiscal year starts on the Sunday closest to December 28.
    Work weeks run Sunday-Saturday.
    
    Parameters
    ----------
    dt : datetime, optional
        Date to calculate work week for. If None, uses current UTC time.
    
    Returns
    -------
    str
        Work week string in format 'YYYYWWNN' (e.g., '2025WW51')
    """
    if dt is None:
        dt = datetime.now(timezone.utc)
    
    # Strip timezone to avoid naive/aware datetime arithmetic issues
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    
    # Get ISO calendar info
    iso_year, iso_week, iso_weekday = dt.isocalendar()
    
    # Intel fiscal year logic
    # Find the Sunday closest to December 28 of the previous calendar year
    dec_28 = datetime(iso_year - 1, 12, 28)
    days_since_sunday = (dec_28.weekday() + 1) % 7
    fiscal_year_start = dec_28 - timedelta(days=days_since_sunday)
    
    # Calculate days since fiscal year start
    days_since_start = (dt - fiscal_year_start).days
    
    # Calculate work week number (1-based)
    fiscal_week = (days_since_start // 7) + 1
    
    # Determine fiscal year
    if dt < fiscal_year_start:
        fiscal_year = iso_year - 1
        # Recalculate for previous fiscal year
        dec_28_prev = datetime(fiscal_year - 1, 12, 28)
        days_since_sunday_prev = (dec_28_prev.weekday() + 1) % 7
        fiscal_year_start_prev = dec_28_prev - timedelta(days=days_since_sunday_prev)
        days_since_start_prev = (dt - fiscal_year_start_prev).days
        fiscal_week = (days_since_start_prev // 7) + 1
    else:
        fiscal_year = iso_year
    
    ww_str = f"{fiscal_year}WW{fiscal_week:02d}"
    logger.debug(f"Calculated Intel work week: {ww_str} for date {dt.date()}")
    return ww_str


def get_recent_work_weeks(num_weeks: int = 4) -> List[str]:
    """
    Get list of recent work week strings.
    
    Parameters
    ----------
    num_weeks : int
        Number of recent weeks to return (including current week)
    
    Returns
    -------
    List[str]
        List of work week strings, most recent first
    """
    current_date = datetime.now(timezone.utc)
    work_weeks = []
    
    for i in range(num_weeks):
        week_date = current_date - timedelta(weeks=i)
        ww_str = get_intel_ww(week_date)
        if ww_str not in work_weeks:  # Avoid duplicates
            work_weeks.append(ww_str)
    
    logger.info(f"Generated {len(work_weeks)} recent work weeks: {work_weeks}")
    return work_weeks


# ============================================================================
# File Discovery
# ============================================================================

def find_entity_states_file(root_path: str, ww_str: str, file_name: str = "EntityStates.csv") -> Optional[Path]:
    """
    Find EntityStates.csv file in work week folder.
    
    Parameters
    ----------
    root_path : str
        Root path to WW folders
    ww_str : str
        Work week string (e.g., '2025WW51')
    file_name : str
        Expected file name
    
    Returns
    -------
    Path or None
        Path to file if found, None otherwise
    """
    root_dir = Path(root_path)
    ww_folder = root_dir / ww_str
    file_path = ww_folder / file_name
    
    if file_path.exists():
        logger.info(f"Found EntityStates file: {file_path}")
        return file_path
    else:
        logger.warning(f"EntityStates file not found: {file_path}")
        return None


def find_latest_counters_file(root_path: str, ww_str: str, file_prefix: str = "Counters_") -> Optional[Tuple[Path, datetime]]:
    """
    Find the most recent Counters_*.csv file in work week folder based on file modified date.
    
    Parameters
    ----------
    root_path : str
        Root path to WW folders
    ww_str : str
        Work week string (e.g., '2025WW51')
    file_prefix : str
        File prefix to search for
    
    Returns
    -------
    Tuple[Path, datetime] or None
        (file_path, modified_datetime) if found, None otherwise
    """
    root_dir = Path(root_path)
    ww_folder = root_dir / ww_str
    
    if not ww_folder.exists():
        logger.warning(f"Work week folder not found: {ww_folder}")
        return None
    
    # Find all files matching prefix
    counter_files = list(ww_folder.glob(f"{file_prefix}*.csv"))
    
    if not counter_files:
        logger.warning(f"No Counters files found in {ww_folder} with prefix '{file_prefix}'")
        return None
    
    # Get file modified times
    file_times = []
    for file_path in counter_files:
        try:
            modified_time = os.path.getmtime(file_path)
            modified_dt = datetime.fromtimestamp(modified_time, tz=timezone.utc)
            file_times.append((file_path, modified_dt))
        except Exception as e:
            logger.error(f"Error getting modified time for {file_path}: {e}")
            continue
    
    if not file_times:
        logger.warning(f"Could not get modified times for any Counters files in {ww_folder}")
        return None
    
    # Sort by modified time (most recent first)
    file_times.sort(key=lambda x: x[1], reverse=True)
    
    most_recent_file, most_recent_time = file_times[0]
    
    logger.info(f"Found {len(file_times)} Counters files in {ww_folder}")
    logger.info(f"Selected most recent: {most_recent_file.name} (modified: {most_recent_time})")
    
    return most_recent_file, most_recent_time


# ============================================================================
# Entity Name Normalization
# ============================================================================

def normalize_entity_name(entity: str, pattern: str = "_PC", replacement: str = "_PM") -> str:
    """
    Normalize entity names by replacing PC with PM.
    
    Parameters
    ----------
    entity : str
        Original entity name
    pattern : str
        Pattern to replace (default: '_PC')
    replacement : str
        Replacement string (default: '_PM')
    
    Returns
    -------
    str
        Normalized entity name
    """
    if pd.isna(entity):
        return entity
    
    normalized = str(entity).replace(pattern, replacement)
    
    if normalized != entity:
        logger.debug(f"Normalized entity: {entity} -> {normalized}")
    
    return normalized


def apply_entity_normalization(df: pd.DataFrame, config: Dict, entity_column: str = 'ENTITY') -> pd.DataFrame:
    """
    Apply entity normalization to a DataFrame.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with entity column
    config : dict
        Configuration dictionary
    entity_column : str
        Name of the entity column
    
    Returns
    -------
    pd.DataFrame
        DataFrame with normalized entity names
    """
    if config['entity_normalization']['replace_pc_with_pm']:
        pattern = config['entity_normalization']['pattern']
        replacement = config['entity_normalization']['replacement']
        
        original_count = len(df)
        entities_changed = df[entity_column] != df[entity_column].str.replace(pattern, replacement)
        changed_count = entities_changed.sum()
        
        df[entity_column] = df[entity_column].apply(
            lambda x: normalize_entity_name(x, pattern, replacement)
        )
        
        logger.info(f"Entity normalization: {changed_count} of {original_count} entities changed ({pattern} -> {replacement})")
    
    return df


# ============================================================================
# FAB_ENTITY Key Creation
# ============================================================================

def create_fab_entity_key(df: pd.DataFrame, fab_column: str = 'FAB', entity_column: str = 'ENTITY') -> pd.DataFrame:
    """
    Create FAB_ENTITY composite key column.
    
    Combines FAB and ENTITY with underscore separator.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with FAB and ENTITY columns
    fab_column : str
        Name of FAB column
    entity_column : str
        Name of ENTITY column
    
    Returns
    -------
    pd.DataFrame
        DataFrame with new FAB_ENTITY column
    """
    df['FAB_ENTITY'] = df[fab_column].astype(str) + '_' + df[entity_column].astype(str)
    
    logger.info(f"Created FAB_ENTITY key from {fab_column} and {entity_column}")
    logger.debug(f"Sample FAB_ENTITY values: {df['FAB_ENTITY'].head(3).tolist()}")
    
    return df


# ============================================================================
# DataFrame Utilities
# ============================================================================

def load_csv_safe(file_path: Path, expected_columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Safely load CSV file with error handling and column validation.
    
    Parameters
    ----------
    file_path : Path
        Path to CSV file
    expected_columns : List[str], optional
        List of expected column names
    
    Returns
    -------
    pd.DataFrame
        Loaded DataFrame
    """
    logger.info(f"Loading CSV: {file_path}")
    
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
    except UnicodeDecodeError:
        logger.warning(f"UTF-8 decode failed, trying latin-1 encoding")
        df = pd.read_csv(file_path, encoding='latin-1')
    
    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    
    # Validate expected columns
    if expected_columns:
        missing_cols = set(expected_columns) - set(df.columns)
        extra_cols = set(df.columns) - set(expected_columns)
        
        if missing_cols:
            logger.warning(f"Missing expected columns: {missing_cols}")
        
        if extra_cols:
            logger.info(f"Extra columns found (dynamic part counters): {len(extra_cols)} columns")
    
    return df


def add_metadata_columns(df: pd.DataFrame, source_file: str, load_ww: str, load_ts: Optional[datetime] = None) -> pd.DataFrame:
    """
    Add standard metadata columns to DataFrame.
    
    Parameters
    ----------
    df : pd.DataFrame
        Source DataFrame
    source_file : str
        Name of source file
    load_ww : str
        Work week string
    load_ts : datetime, optional
        Load timestamp (defaults to current UTC time)
    
    Returns
    -------
    pd.DataFrame
        DataFrame with metadata columns added
    """
    if load_ts is None:
        load_ts = datetime.now(timezone.utc)
    
    df['source_file'] = source_file
    df['load_ww'] = load_ww
    df['load_ts'] = load_ts.isoformat()
    
    logger.debug(f"Added metadata columns: source_file={source_file}, load_ww={load_ww}, load_ts={load_ts}")
    
    return df


def adjust_timestamp(dt: datetime, days: int) -> datetime:
    """
    Adjust timestamp by specified number of days.
    
    Parameters
    ----------
    dt : datetime
        Original timestamp
    days : int
        Number of days to add (negative to subtract)
    
    Returns
    -------
    datetime
        Adjusted timestamp
    """
    adjusted = dt + timedelta(days=days)
    logger.debug(f"Adjusted timestamp: {dt} -> {adjusted} ({days:+d} days)")
    return adjusted













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
import re
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
        self.exclude_patterns = self.wafer_config.get('exclude_patterns', [])
        
        # Initialize specialized logger
        self.prod_logger = WaferProductionLogger(logger, config['logging'])
        
        logger.info("Wafer Production Calculator initialized")
        logger.info(f"Primary keywords: {self.primary_keywords}")
        logger.info(f"Fallback keywords: {self.fallback_keywords}")
        logger.info(f"Replacement threshold: {self.replacement_threshold}")
        logger.info(f"Exclude patterns: {self.exclude_patterns}")
    
    def is_entity_excluded(self, entity: str) -> bool:
        """
        Check if entity should be excluded from wafer production calculation.
        
        Parameters
        ----------
        entity : str
            Entity name to check
        
        Returns
        -------
        bool
            True if entity should be excluded, False otherwise
        """
        for pattern in self.exclude_patterns:
            if re.search(pattern, entity):
                return True
        return False
    
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
        
        # Filter out excluded entities (e.g., loadports)
        if self.exclude_patterns:
            initial_count = len(counters_df)
            excluded_entities = counters_df['ENTITY'].apply(self.is_entity_excluded)
            excluded_count = excluded_entities.sum()
            
            if excluded_count > 0:
                excluded_list = counters_df[excluded_entities]['ENTITY'].unique().tolist()
                logger.info(f"Excluding {excluded_count} rows from {len(excluded_list)} entities matching exclude patterns")
                logger.info(f"Sample excluded entities: {excluded_list[:5]}")
                counters_df = counters_df[~excluded_entities].reset_index(drop=True)
                logger.info(f"Remaining entities for wafer production: {len(counters_df)}")
        
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
