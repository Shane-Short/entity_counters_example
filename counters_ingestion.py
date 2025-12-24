"""
Bronze Layer - Counters File Ingestion
=======================================
Loads Counters_*.csv files from work week folders into raw bronze table.

Features:
- Finds latest Counters file by modified date
- Adjusts timestamp (subtracts 1 day)
- Loads last 4 weeks of historical data
- Applies entity normalization (PC -> PM)
- Creates FAB_ENTITY key
- Handles dynamic part counter columns
- Full and incremental refresh modes
"""

import pandas as pd
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
import yaml

# Import helper functions
from utils.helpers import (
    get_recent_work_weeks,
    find_latest_counters_file,
    normalize_entity_name,
    apply_entity_normalization,
    create_fab_entity_key,
    load_csv_safe,
    add_metadata_columns,
    adjust_timestamp
)
from utils.logger import setup_logger

logger = logging.getLogger(__name__)


class CountersIngestion:
    """
    Handles ingestion of Counters_*.csv files into Bronze layer.
    """
    
    def __init__(self, config: Dict):
        """
        Initialize Counters ingestion.
        
        Parameters
        ----------
        config : dict
            Configuration dictionary from config.yaml
        """
        self.config = config
        self.source_config = config['entity_counters_source']
        self.counters_config = self.source_config['counters']
        self.historical_config = config['historical_load']
        
        self.logger = logger
        self.logger.info("Counters Ingestion initialized")
    
    def discover_files(self, mode: str = 'full') -> List[Tuple[str, Path, datetime]]:
        """
        Discover Counters files to process.
        
        Parameters
        ----------
        mode : str
            'full' = load last N weeks, 'incremental' = load current week only
        
        Returns
        -------
        List[Tuple[str, Path, datetime]]
            List of (ww_string, file_path, modified_datetime) tuples
        """
        root_path = self.source_config['root_path']
        file_prefix = self.counters_config['file_prefix']
        
        if mode == 'full' and self.historical_config['enabled']:
            # Load last N weeks
            num_weeks = self.historical_config['weeks_to_load']
            work_weeks = get_recent_work_weeks(num_weeks)
            self.logger.info(f"Full refresh mode: Loading {num_weeks} weeks of data")
        else:
            # Load current week only
            work_weeks = get_recent_work_weeks(1)
            self.logger.info(f"Incremental mode: Loading current week only")
        
        # Find latest Counters file for each work week
        files_to_process = []
        for ww_str in work_weeks:
            result = find_latest_counters_file(root_path, ww_str, file_prefix)
            if result:
                file_path, modified_dt = result
                files_to_process.append((ww_str, file_path, modified_dt))
            else:
                self.logger.warning(f"No Counters file found for {ww_str}")
        
        self.logger.info(f"Discovered {len(files_to_process)} Counters files to process")
        return files_to_process
    
    def load_single_file(self, ww_str: str, file_path: Path, modified_dt: datetime) -> pd.DataFrame:
        """
        Load a single Counters file.
        
        Parameters
        ----------
        ww_str : str
            Work week string
        file_path : Path
            Path to CSV file
        modified_dt : datetime
            File modified timestamp
        
        Returns
        -------
        pd.DataFrame
            Loaded and processed DataFrame
        """
        self.logger.info(f"Loading Counters file for {ww_str}: {file_path.name}")
        self.logger.info(f"File modified: {modified_dt}")
        
        # Load CSV
        df = load_csv_safe(file_path, expected_columns=None)  # No expected columns (dynamic)
        
        # Validate ENTITY column exists
        if 'ENTITY' not in df.columns:
            raise ValueError(f"ENTITY column not found in {file_path}")
        
        self.logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns from {file_path.name}")
        
        # Apply entity normalization (PC -> PM)
        df = apply_entity_normalization(df, self.config, entity_column='ENTITY')
        
        # Add FAB column if not present (required for FAB_ENTITY key)
        if 'FAB' not in df.columns:
            # Extract FAB from file path or use default
            # Counters files typically don't have FAB column, need to derive it
            # For now, we'll extract from ENTITY or use a default
            # This may need refinement based on actual data structure
            self.logger.warning("FAB column not found in Counters file - will need to derive from ENTITY or other source")
            # Placeholder: extract first part of ENTITY before underscore
            df['FAB'] = df['ENTITY'].str.split('_').str[0]
        
        # Create FAB_ENTITY key
        df = create_fab_entity_key(df, fab_column='FAB', entity_column='ENTITY')
        
        # Calculate adjusted timestamp (subtract N days from file modified date)
        date_adjustment = self.counters_config.get('date_adjustment_days', -1)
        adjusted_ts = adjust_timestamp(modified_dt, days=date_adjustment)
        
        # Add metadata columns
        df = add_metadata_columns(
            df,
            source_file=file_path.name,
            load_ww=ww_str,
            load_ts=datetime.now(timezone.utc)
        )
        
        # Add counter_date column (adjusted timestamp date)
        df['counter_date'] = adjusted_ts.date()
        df['file_modified_ts'] = modified_dt.isoformat()
        
        self.logger.info(f"Counter date (adjusted): {adjusted_ts.date()}")
        
        return df
    
    def load_all_files(self, mode: str = 'full') -> pd.DataFrame:
        """
        Load all Counters files.
        
        Parameters
        ----------
        mode : str
            'full' or 'incremental'
        
        Returns
        -------
        pd.DataFrame
            Combined DataFrame from all files
        """
        self.logger.info(f"Starting Counters ingestion (mode: {mode})")
        
        # Discover files
        files_to_process = self.discover_files(mode)
        
        if not files_to_process:
            self.logger.warning("No Counters files found to process")
            return pd.DataFrame()
        
        # Load all files
        all_dfs = []
        for ww_str, file_path, modified_dt in files_to_process:
            try:
                df = self.load_single_file(ww_str, file_path, modified_dt)
                all_dfs.append(df)
            except Exception as e:
                self.logger.error(f"Error loading {file_path}: {e}")
                continue
        
        if not all_dfs:
            self.logger.error("No Counters files loaded successfully")
            return pd.DataFrame()
        
        # Combine all DataFrames
        # Note: Different files may have different part counter columns
        combined_df = pd.concat(all_dfs, ignore_index=True, sort=False)
        
        # Remove duplicates before loading to database
        # Deduplicate on: FAB_ENTITY + counter_date
        # Keep the most recent load (last occurrence)
        before_dedup = len(combined_df)
        combined_df = combined_df.drop_duplicates(
            subset=['FAB_ENTITY', 'counter_date'],
            keep='last'
        )
        after_dedup = len(combined_df)
        
        if before_dedup > after_dedup:
            self.logger.info(f"Removed {before_dedup - after_dedup} duplicate rows before database load")
        
        # Log column count
        total_columns = len(combined_df.columns)
        part_counter_columns = [col for col in combined_df.columns 
                               if col not in ['ENTITY', 'FAB', 'FAB_ENTITY', 
                                            'source_file', 'load_ww', 'load_ts', 
                                            'counter_date', 'file_modified_ts']]
        
        self.logger.info(f"Counters ingestion complete: {len(combined_df)} total rows from {len(all_dfs)} files")
        self.logger.info(f"Total columns: {total_columns} (including {len(part_counter_columns)} part counter columns)")
        
        return combined_df
    
    def run(self, mode: str = 'full') -> pd.DataFrame:
        """
        Main entry point for Counters ingestion.
        
        Parameters
        ----------
        mode : str
            'full' or 'incremental'
        
        Returns
        -------
        pd.DataFrame
            Final DataFrame ready for Bronze table
        """
        return self.load_all_files(mode)


def run_counters_ingestion(config: Dict, mode: str = 'full') -> pd.DataFrame:
    """
    Standalone function to run Counters ingestion.
    
    Parameters
    ----------
    config : dict
        Configuration dictionary
    mode : str
        'full' or 'incremental'
    
    Returns
    -------
    pd.DataFrame
        Processed Counters data
    """
    ingestion = CountersIngestion(config)
    return ingestion.run(mode)


if __name__ == "__main__":
    # Test script
    import sys
    
    # Setup logging
    setup_logger(__name__, level='DEBUG')
    
    # Load config
    config_path = Path(__file__).parent.parent.parent / 'config' / 'config.yaml'
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Run ingestion
    mode = sys.argv[1] if len(sys.argv) > 1 else 'full'
    df = run_counters_ingestion(config, mode=mode)
    
    print(f"\nCounters Ingestion Results:")
    print(f"Total rows: {len(df)}")
    print(f"Columns: {list(df.columns)[:10]}...")  # Show first 10 columns
    print(f"\nSample data:")
    print(df.head())
    print(f"\nPart counter columns found: {len([c for c in df.columns if c.endswith('Counter')])}")
