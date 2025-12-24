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
    logger.info(f"Calculated Intel work week: {ww_str} for date {dt.date()}")
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
