"""
Bronze Layer - EntityStates.csv Ingestion
==========================================
Loads EntityStates.csv files from work week folders into raw bronze table.

Features:
- Discovers EntityStates.csv in WW folders
- Loads last 4 weeks of historical data
- Applies entity normalization (PC -> PM)
- Creates FAB_ENTITY key
- Adds metadata columns
- Full and incremental refresh modes
"""

import pandas as pd
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional
import yaml

# Import helper functions
from utils.helpers import (
    get_recent_work_weeks,
    find_entity_states_file,
    normalize_entity_name,
    apply_entity_normalization,
    create_fab_entity_key,
    load_csv_safe,
    add_metadata_columns
)
from utils.logger import setup_logger

logger = logging.getLogger(__name__)


class EntityStatesIngestion:
    """
    Handles ingestion of EntityStates.csv files into Bronze layer.
    """
    
    def __init__(self, config: Dict):
        """
        Initialize EntityStates ingestion.
        
        Parameters
        ----------
        config : dict
            Configuration dictionary from config.yaml
        """
        self.config = config
        self.source_config = config['entity_counters_source']
        self.entity_states_config = self.source_config['entity_states']
        self.historical_config = config['historical_load']
        
        self.logger = logger
        self.logger.info("EntityStates Ingestion initialized")
    
    def discover_files(self, mode: str = 'full') -> List[tuple]:
        """
        Discover EntityStates.csv files to process.
        
        Parameters
        ----------
        mode : str
            'full' = load last N weeks, 'incremental' = load current week only
        
        Returns
        -------
        List[tuple]
            List of (ww_string, file_path) tuples
        """
        root_path = self.source_config['root_path']
        file_name = self.entity_states_config['file_name']
        
        if mode == 'full' and self.historical_config['enabled']:
            # Load last N weeks
            num_weeks = self.historical_config['weeks_to_load']
            work_weeks = get_recent_work_weeks(num_weeks)
            self.logger.info(f"Full refresh mode: Loading {num_weeks} weeks of data")
        else:
            # Load current week only
            work_weeks = get_recent_work_weeks(1)
            self.logger.info(f"Incremental mode: Loading current week only")
        
        # Find files for each work week
        files_to_process = []
        for ww_str in work_weeks:
            file_path = find_entity_states_file(root_path, ww_str, file_name)
            if file_path:
                files_to_process.append((ww_str, file_path))
            else:
                self.logger.warning(f"EntityStates file not found for {ww_str}")
        
        self.logger.info(f"Discovered {len(files_to_process)} EntityStates files to process")
        return files_to_process
    
    def load_single_file(self, ww_str: str, file_path: Path) -> pd.DataFrame:
        """
        Load a single EntityStates.csv file.
        
        Parameters
        ----------
        ww_str : str
            Work week string
        file_path : Path
            Path to CSV file
        
        Returns
        -------
        pd.DataFrame
            Loaded and processed DataFrame
        """
        self.logger.info(f"Loading EntityStates file for {ww_str}: {file_path}")
        
        # Load CSV
        expected_cols = self.entity_states_config['expected_columns']
        df = load_csv_safe(file_path, expected_columns=expected_cols)
        
        # Validate required columns exist
        required_cols = ['FAB', 'ENTITY', 'ENTITY_STATE', 'HOURS_IN_STATE']
        missing_required = set(required_cols) - set(df.columns)
        if missing_required:
            raise ValueError(f"Missing required columns: {missing_required}")
        
        self.logger.info(f"Loaded {len(df)} rows from {file_path.name}")
        
        # Apply entity normalization (PC -> PM)
        df = apply_entity_normalization(df, self.config, entity_column='ENTITY')
        
        # Create FAB_ENTITY key
        df = create_fab_entity_key(df, fab_column='FAB', entity_column='ENTITY')
        
        # Add metadata columns
        df = add_metadata_columns(
            df,
            source_file=file_path.name,
            load_ww=ww_str,
            load_ts=datetime.now(timezone.utc)
        )
        
        # Add load_date column (date when data was collected)
        # Parse from DAY_SHIFT column if possible, otherwise use load_ts
        df['load_date'] = datetime.now(timezone.utc).date()
        
        return df
    
    def load_all_files(self, mode: str = 'full') -> pd.DataFrame:
        """
        Load all EntityStates files.
        
        Parameters
        ----------
        mode : str
            'full' or 'incremental'
        
        Returns
        -------
        pd.DataFrame
            Combined DataFrame from all files
        """
        self.logger.info(f"Starting EntityStates ingestion (mode: {mode})")
        
        # Discover files
        files_to_process = self.discover_files(mode)
        
        if not files_to_process:
            self.logger.warning("No EntityStates files found to process")
            return pd.DataFrame()
        
        # Load all files
        all_dfs = []
        for ww_str, file_path in files_to_process:
            try:
                df = self.load_single_file(ww_str, file_path)
                all_dfs.append(df)
            except Exception as e:
                self.logger.error(f"Error loading {file_path}: {e}")
                continue
        
        if not all_dfs:
            self.logger.error("No EntityStates files loaded successfully")
            return pd.DataFrame()
        
        # Combine all DataFrames
        combined_df = pd.concat(all_dfs, ignore_index=True)
        
        # Remove duplicates before loading to database
        # Deduplicate on: FAB_ENTITY + DAY_SHIFT + ENTITY_STATE
        # Keep the most recent load (last occurrence)
        before_dedup = len(combined_df)
        combined_df = combined_df.drop_duplicates(
            subset=['FAB_ENTITY', 'DAY_SHIFT', 'ENTITY_STATE'],
            keep='last'
        )
        after_dedup = len(combined_df)
        
        if before_dedup > after_dedup:
            self.logger.info(f"Removed {before_dedup - after_dedup} duplicate rows before database load")
        
        self.logger.info(f"EntityStates ingestion complete: {len(combined_df)} total rows from {len(all_dfs)} files")
        
        return combined_df
    
    def run(self, mode: str = 'full') -> pd.DataFrame:
        """
        Main entry point for EntityStates ingestion.
        
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


def run_entity_states_ingestion(config: Dict, mode: str = 'full') -> pd.DataFrame:
    """
    Standalone function to run EntityStates ingestion.
    
    Parameters
    ----------
    config : dict
        Configuration dictionary
    mode : str
        'full' or 'incremental'
    
    Returns
    -------
    pd.DataFrame
        Processed EntityStates data
    """
    ingestion = EntityStatesIngestion(config)
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
    df = run_entity_states_ingestion(config, mode=mode)
    
    print(f"\nEntityStates Ingestion Results:")
    print(f"Total rows: {len(df)}")
    print(f"Columns: {list(df.columns)}")
    print(f"\nSample data:")
    print(df.head())
