"""
Dynamic Counters Table Setup
=============================
Reads a sample Counters_*.csv file and generates CREATE TABLE SQL script
with ALL actual columns from the CSV file.

This ensures we capture EVERY part counter column, not just sample columns.

Usage:
    python -m etl.setup_counters_table
    
This will:
1. Find a sample Counters CSV file
2. Read all column names
3. Generate create_counters_raw_GENERATED.sql with full schema
4. You then execute that generated SQL file
"""

import pandas as pd
import yaml
import logging
from pathlib import Path
from datetime import datetime

from utils.helpers import get_recent_work_weeks, find_latest_counters_file
from utils.logger import setup_logger

logger = setup_logger(__name__, level='INFO')


class CountersTableSetup:
    """
    Generates CREATE TABLE SQL for counters_raw based on actual CSV columns.
    """
    
    def __init__(self, config_path: str = 'config/config.yaml'):
        """
        Initialize setup.
        
        Parameters
        ----------
        config_path : str
            Path to configuration file
        """
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.source_config = self.config['entity_counters_source']
        logger.info("Counters Table Setup initialized")
    
    def find_sample_counters_file(self):
        """
        Find a sample Counters CSV file to read column names from.
        
        Returns
        -------
        Path
            Path to sample Counters file
        """
        logger.info("Searching for sample Counters file...")
        
        root_path = self.source_config['root_path']
        file_prefix = self.source_config['counters']['file_prefix']
        
        # Try last 4 weeks
        work_weeks = get_recent_work_weeks(4)
        
        for ww_str in work_weeks:
            result = find_latest_counters_file(root_path, ww_str, file_prefix)
            if result:
                file_path, modified_dt = result
                logger.info(f"Found sample file: {file_path}")
                return file_path
        
        raise FileNotFoundError("No Counters CSV file found in last 4 weeks")
    
    def read_column_names(self, file_path: Path):
        """
        Read column names from Counters CSV.
        
        Parameters
        ----------
        file_path : Path
            Path to Counters CSV
        
        Returns
        -------
        list
            List of column names
        """
        logger.info(f"Reading columns from: {file_path}")
        
        # Read just the first row to get columns
        df = pd.read_csv(file_path, nrows=1)
        columns = df.columns.tolist()
        
        logger.info(f"Found {len(columns)} columns in CSV")
        logger.info(f"Sample columns: {columns[:10]}")
        
        return columns
    
    def generate_sql_script(self, csv_columns: list, output_path: str = 'sql/ddl/create_counters_raw_GENERATED.sql'):
        """
        Generate CREATE TABLE SQL script with all columns.
        
        Parameters
        ----------
        csv_columns : list
            List of column names from CSV
        output_path : str
            Where to save generated SQL
        """
        logger.info("Generating CREATE TABLE SQL script...")
        
        # Required system columns
        system_columns = ['ENTITY', 'FAB']
        
        # Part counter columns (everything else from CSV)
        part_counter_columns = [col for col in csv_columns if col not in system_columns]
        
        # Verify required columns exist
        if 'ENTITY' not in csv_columns:
            raise ValueError("ENTITY column not found in Counters CSV!")
        
        # FAB might not be in CSV - we'll add it
        has_fab = 'FAB' in csv_columns
        if not has_fab:
            logger.warning("FAB column not in CSV - will derive from ENTITY")
        
        # Build SQL script
        sql_lines = []
        sql_lines.append("-- " + "=" * 76)
        sql_lines.append("-- AUTO-GENERATED counters_raw Table Definition")
        sql_lines.append("-- " + "=" * 76)
        sql_lines.append(f"-- Generated: {datetime.now()}")
        sql_lines.append(f"-- Source CSV columns: {len(csv_columns)}")
        sql_lines.append(f"-- Part counter columns: {len(part_counter_columns)}")
        sql_lines.append("-- " + "=" * 76)
        sql_lines.append("")
        sql_lines.append("USE Parts_Counter_Production;")
        sql_lines.append("GO")
        sql_lines.append("")
        sql_lines.append("DROP TABLE IF EXISTS dbo.counters_raw;")
        sql_lines.append("GO")
        sql_lines.append("")
        sql_lines.append("CREATE TABLE dbo.counters_raw (")
        sql_lines.append("    -- Primary Key")
        sql_lines.append("    counters_raw_id INT IDENTITY(1,1) PRIMARY KEY,")
        sql_lines.append("    ")
        sql_lines.append("    -- System Columns")
        sql_lines.append("    ENTITY VARCHAR(255) NOT NULL,")
        
        if has_fab:
            sql_lines.append("    FAB VARCHAR(50) NOT NULL,")
        else:
            sql_lines.append("    FAB VARCHAR(50) NOT NULL,  -- Derived from ENTITY in pipeline")
        
        sql_lines.append("    FAB_ENTITY VARCHAR(300) NOT NULL,")
        sql_lines.append("    ")
        sql_lines.append(f"    -- Part Counter Columns ({len(part_counter_columns)} columns from CSV)")
        
        # Add each part counter column
        for i, col in enumerate(part_counter_columns):
            # Clean column name (escape brackets if needed)
            clean_col = col.replace('[', '').replace(']', '')
            
            # Determine if this is last counter column
            is_last_counter = (i == len(part_counter_columns) - 1)
            
            if is_last_counter:
                sql_lines.append(f"    [{clean_col}] DECIMAL(18,2),")
            else:
                sql_lines.append(f"    [{clean_col}] DECIMAL(18,2),")
        
        sql_lines.append("    ")
        sql_lines.append("    -- Metadata Columns")
        sql_lines.append("    source_file VARCHAR(500),")
        sql_lines.append("    load_ww VARCHAR(20),")
        sql_lines.append("    load_ts DATETIME2(7),")
        sql_lines.append("    counter_date DATE NOT NULL,")
        sql_lines.append("    file_modified_ts DATETIME2(7),")
        sql_lines.append("    ")
        sql_lines.append("    -- Indexes")
        sql_lines.append("    INDEX IX_counters_raw_FAB_ENTITY (FAB_ENTITY),")
        sql_lines.append("    INDEX IX_counters_raw_ENTITY (ENTITY),")
        sql_lines.append("    INDEX IX_counters_raw_counter_date (counter_date),")
        sql_lines.append("    INDEX IX_counters_raw_FAB_ENTITY_date (FAB_ENTITY, counter_date)")
        sql_lines.append(");")
        sql_lines.append("GO")
        sql_lines.append("")
        sql_lines.append("PRINT 'counters_raw table created successfully';")
        sql_lines.append(f"PRINT 'Total columns: {len(csv_columns) + 5}';  -- CSV columns + metadata")
        sql_lines.append(f"PRINT 'Part counter columns: {len(part_counter_columns)}';")
        sql_lines.append("GO")
        
        # Write to file
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w') as f:
            f.write('\n'.join(sql_lines))
        
        logger.info(f"Generated SQL script: {output_file}")
        logger.info(f"Total columns in table: {len(csv_columns) + 5}")
        logger.info(f"Part counter columns: {len(part_counter_columns)}")
        
        return output_file
    
    def run(self):
        """
        Main execution: Find sample file, read columns, generate SQL.
        """
        logger.info("=" * 80)
        logger.info("COUNTERS TABLE DYNAMIC SETUP")
        logger.info("=" * 80)
        
        # Find sample file
        sample_file = self.find_sample_counters_file()
        
        # Read columns
        csv_columns = self.read_column_names(sample_file)
        
        # Generate SQL
        output_file = self.generate_sql_script(csv_columns)
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("SETUP COMPLETE")
        logger.info("=" * 80)
        logger.info("")
        logger.info("NEXT STEPS:")
        logger.info(f"1. Open SQL Server Management Studio")
        logger.info(f"2. Execute: {output_file}")
        logger.info(f"3. This will create counters_raw table with ALL {len(csv_columns)} columns")
        logger.info(f"4. Then run the ETL pipeline normally")
        logger.info("=" * 80)


if __name__ == "__main__":
    setup = CountersTableSetup()
    setup.run()










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
        
        # Handle FAB column
        if 'FAB' not in df.columns:
            # FAB not in CSV - derive from ENTITY
            # Typical ENTITY format: "FAB_TOOL_PM" or "FACILITYNAME_TOOL_PM"
            # Extract first part before underscore as FAB
            self.logger.warning("FAB column not found in Counters file - deriving from ENTITY")
            df['FAB'] = df['ENTITY'].str.split('_').str[0]
            self.logger.info(f"Derived FAB from ENTITY - Sample FAB values: {df['FAB'].unique()[:5].tolist()}")
        else:
            self.logger.info("FAB column found in CSV - using as-is")
        
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












    -- ============================================================================
-- Bronze Layer - Table Definitions
-- ============================================================================
-- Database: Parts_Counter_Production
-- Schema: dbo
-- Purpose: Raw mirror tables for EntityStates.csv and Counters_*.csv files
-- ============================================================================

USE Parts_Counter_Production;
GO

-- ============================================================================
-- entity_states_raw
-- ============================================================================
-- Purpose: Raw mirror of EntityStates.csv files
-- Grain: One row per entity per state per shift per day
-- Source: EntityStates.csv from weekly WW folders
-- ============================================================================

DROP TABLE IF EXISTS dbo.entity_states_raw;
GO

CREATE TABLE dbo.entity_states_raw (
    -- Primary Key
    entity_state_raw_id INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Source Data Columns
    FAB VARCHAR(50) NOT NULL,
    WW VARCHAR(20),
    DAY_SHIFT VARCHAR(100) NOT NULL,
    ENTITY_STATE VARCHAR(100) NOT NULL,
    ENTITY VARCHAR(255) NOT NULL,
    HOURS_IN_STATE DECIMAL(10,2),
    Total_Hours DECIMAL(10,2),
    [% in State] DECIMAL(10,4),
    
    -- Derived Columns
    FAB_ENTITY VARCHAR(300) NOT NULL,
    
    -- Metadata Columns
    source_file VARCHAR(500),
    load_ww VARCHAR(20),
    load_ts DATETIME2(7),
    load_date DATE,
    
    -- Indexes for common queries
    INDEX IX_entity_states_raw_FAB_ENTITY (FAB_ENTITY),
    INDEX IX_entity_states_raw_ENTITY (ENTITY),
    INDEX IX_entity_states_raw_DAY_SHIFT (DAY_SHIFT),
    INDEX IX_entity_states_raw_load_date (load_date)
);
GO

PRINT 'Table created: dbo.entity_states_raw';
GO


-- ============================================================================
-- counters_raw
-- ============================================================================
-- Purpose: Raw mirror of Counters_*.csv files with ALL part counter columns
-- Grain: One row per entity per counter date
-- Source: Counters_*.csv from weekly WW folders (latest by modified date)
-- 
-- IMPORTANT: This table MUST be created dynamically on first pipeline run
-- based on actual CSV columns. The script below will be generated by the
-- Python pipeline during initial setup.
-- 
-- Manual Setup Instructions:
-- 1. Run the Python pipeline setup script FIRST: python -m etl.setup_counters_table
-- 2. It will read a sample Counters CSV and generate the full CREATE TABLE
-- 3. Then execute the generated script
-- 
-- DO NOT create this table manually with sample columns - you will lose data!
-- ============================================================================

-- This is a PLACEHOLDER - The actual table will be created by setup_counters_table.py
-- which reads the actual CSV columns and generates the full DDL

PRINT '============================================================================';
PRINT 'WARNING: counters_raw table requires DYNAMIC creation';
PRINT '============================================================================';
PRINT '';
PRINT 'DO NOT create this table with sample columns!';
PRINT '';
PRINT 'REQUIRED SETUP STEPS:';
PRINT '1. Ensure a sample Counters_*.csv file exists in a WW folder';
PRINT '2. Run: python -m etl.setup_counters_table';
PRINT '3. This will generate: sql/ddl/create_counters_raw_GENERATED.sql';
PRINT '4. Execute the generated script to create the full table with ALL columns';
PRINT '';
PRINT 'The generated table will include:';
PRINT '  - ENTITY VARCHAR(255)';
PRINT '  - FAB VARCHAR(50)';
PRINT '  - FAB_ENTITY VARCHAR(300)';
PRINT '  - ALL part counter columns from CSV (300+ columns)';
PRINT '  - source_file, load_ww, load_ts, counter_date, file_modified_ts';
PRINT '';
PRINT 'Skipping counters_raw creation - use setup script instead.';
PRINT '============================================================================';
GO

-- ============================================================================
-- Alternative: JSON-based Counters Table (More Flexible)
-- ============================================================================
-- Uncomment this section if you want to use JSON storage for dynamic columns
/*
DROP TABLE IF EXISTS dbo.counters_raw;
GO

CREATE TABLE dbo.counters_raw (
    counters_raw_id INT IDENTITY(1,1) PRIMARY KEY,
    ENTITY VARCHAR(255) NOT NULL,
    FAB VARCHAR(50) NOT NULL,
    FAB_ENTITY VARCHAR(300) NOT NULL,
    
    -- Store all part counters as JSON
    part_counters NVARCHAR(MAX),
    
    -- Metadata
    source_file VARCHAR(500),
    load_ww VARCHAR(20),
    load_ts DATETIME2(7),
    counter_date DATE NOT NULL,
    file_modified_ts DATETIME2(7),
    
    INDEX IX_counters_raw_FAB_ENTITY (FAB_ENTITY),
    INDEX IX_counters_raw_counter_date (counter_date)
);
GO
*/

-- ============================================================================
-- Bronze Layer Tables Complete
-- ============================================================================
PRINT '';
PRINT 'Bronze layer tables created successfully.';
PRINT 'Tables: entity_states_raw, counters_raw';
GO










-- ============================================================================
-- Master Database Setup Script
-- ============================================================================
-- Entity States & Counters Pipeline
-- Database: Parts_Counter_Production
-- 
-- Purpose: Creates all tables for Bronze, Silver, and Gold layers
-- 
-- Usage:
--   Execute this script in SQL Server Management Studio
--   OR
--   Run individual layer scripts in order:
--     1. create_bronze_tables.sql
--     2. create_silver_tables.sql
--     3. create_gold_tables.sql
-- ============================================================================

USE Parts_Counter_Production;
GO

PRINT '========================================';
PRINT 'Entity & Counters Pipeline Setup';
PRINT 'Starting database initialization...';
PRINT '========================================';
PRINT '';
GO

-- ============================================================================
-- BRONZE LAYER - Raw Data Tables
-- ============================================================================

PRINT '========================================';
PRINT 'Creating BRONZE layer tables...';
PRINT '========================================';
PRINT '';
GO

-- entity_states_raw
DROP TABLE IF EXISTS dbo.entity_states_raw;
GO

CREATE TABLE dbo.entity_states_raw (
    entity_state_raw_id INT IDENTITY(1,1) PRIMARY KEY,
    FAB VARCHAR(50) NOT NULL,
    WW VARCHAR(20),
    DAY_SHIFT VARCHAR(100) NOT NULL,
    ENTITY_STATE VARCHAR(100) NOT NULL,
    ENTITY VARCHAR(255) NOT NULL,
    HOURS_IN_STATE DECIMAL(10,2),
    Total_Hours DECIMAL(10,2),
    [% in State] DECIMAL(10,4),
    FAB_ENTITY VARCHAR(300) NOT NULL,
    source_file VARCHAR(500),
    load_ww VARCHAR(20),
    load_ts DATETIME2(7),
    load_date DATE,
    INDEX IX_entity_states_raw_FAB_ENTITY (FAB_ENTITY),
    INDEX IX_entity_states_raw_ENTITY (ENTITY),
    INDEX IX_entity_states_raw_DAY_SHIFT (DAY_SHIFT),
    INDEX IX_entity_states_raw_load_date (load_date)
);
GO

PRINT 'Created: dbo.entity_states_raw';
GO

-- counters_raw
-- IMPORTANT: This table requires DYNAMIC creation based on actual CSV columns
-- DO NOT create with sample columns - you will lose data!
-- Run: python -m etl.setup_counters_table FIRST to generate the full DDL

PRINT 'Skipping counters_raw - requires dynamic setup';
PRINT 'Run: python -m etl.setup_counters_table to generate full DDL';
PRINT '';
GO

-- ============================================================================
-- SILVER LAYER - Enriched Data Tables
-- ============================================================================

PRINT '========================================';
PRINT 'Creating SILVER layer tables...';
PRINT '========================================';
PRINT '';
GO

-- state_hours
DROP TABLE IF EXISTS dbo.state_hours;
GO

CREATE TABLE dbo.state_hours (
    state_hours_id INT IDENTITY(1,1) PRIMARY KEY,
    ENTITY VARCHAR(255) NOT NULL,
    FAB VARCHAR(50) NOT NULL,
    FAB_ENTITY VARCHAR(300) NOT NULL,
    state_date DATE NOT NULL,
    running_hours DECIMAL(10,2) DEFAULT 0,
    idle_hours DECIMAL(10,2) DEFAULT 0,
    down_hours DECIMAL(10,2) DEFAULT 0,
    bagged_hours DECIMAL(10,2) DEFAULT 0,
    total_hours DECIMAL(10,2) DEFAULT 0,
    is_bagged BIT DEFAULT 0,
    calculation_timestamp DATETIME2(7),
    INDEX IX_state_hours_ENTITY_date (ENTITY, state_date),
    INDEX IX_state_hours_FAB_ENTITY (FAB_ENTITY),
    INDEX IX_state_hours_state_date (state_date),
    CONSTRAINT UQ_state_hours_ENTITY_date UNIQUE (ENTITY, state_date)
);
GO

PRINT 'Created: dbo.state_hours';
GO

-- wafer_production
DROP TABLE IF EXISTS dbo.wafer_production;
GO

CREATE TABLE dbo.wafer_production (
    wafer_production_id INT IDENTITY(1,1) PRIMARY KEY,
    ENTITY VARCHAR(255) NOT NULL,
    counter_date DATE NOT NULL,
    counter_column_used VARCHAR(255),
    counter_keyword_used VARCHAR(100),
    counter_current_value DECIMAL(18,2),
    counter_previous_value DECIMAL(18,2),
    counter_change DECIMAL(18,2),
    part_replacement_detected BIT DEFAULT 0,
    wafers_produced DECIMAL(18,2),
    running_hours DECIMAL(10,2),
    wafers_per_hour DECIMAL(18,4),
    calculation_notes VARCHAR(MAX),
    calculation_timestamp DATETIME2(7),
    INDEX IX_wafer_production_ENTITY_date (ENTITY, counter_date),
    INDEX IX_wafer_production_counter_date (counter_date),
    INDEX IX_wafer_production_replacements (part_replacement_detected),
    CONSTRAINT UQ_wafer_production_ENTITY_date UNIQUE (ENTITY, counter_date)
);
GO

PRINT 'Created: dbo.wafer_production';
GO

-- part_replacements
DROP TABLE IF EXISTS dbo.part_replacements;
GO

CREATE TABLE dbo.part_replacements (
    part_replacement_id INT IDENTITY(1,1) PRIMARY KEY,
    ENTITY VARCHAR(255) NOT NULL,
    replacement_date DATE NOT NULL,
    part_counter_name VARCHAR(255) NOT NULL,
    last_value_before_replacement DECIMAL(18,2),
    first_value_after_replacement DECIMAL(18,2),
    value_drop DECIMAL(18,2),
    part_wafers_at_replacement DECIMAL(18,2),
    notes VARCHAR(MAX),
    replacement_detected_ts DATETIME2(7),
    INDEX IX_part_replacements_ENTITY (ENTITY),
    INDEX IX_part_replacements_date (replacement_date),
    INDEX IX_part_replacements_part (part_counter_name),
    INDEX IX_part_replacements_ENTITY_date (ENTITY, replacement_date),
    CONSTRAINT UQ_part_replacements_ENTITY_date_part UNIQUE (ENTITY, replacement_date, part_counter_name)
);
GO

PRINT 'Created: dbo.part_replacements';
PRINT '';
GO

-- ============================================================================
-- GOLD LAYER - KPI Fact Tables
-- ============================================================================

PRINT '========================================';
PRINT 'Creating GOLD layer tables...';
PRINT '========================================';
PRINT '';
GO

-- fact_daily_production
DROP TABLE IF EXISTS dbo.fact_daily_production;
GO

CREATE TABLE dbo.fact_daily_production (
    daily_production_id INT IDENTITY(1,1) PRIMARY KEY,
    ENTITY VARCHAR(255) NOT NULL,
    FAB VARCHAR(50) NOT NULL,
    FAB_ENTITY VARCHAR(300) NOT NULL,
    production_date DATE NOT NULL,
    wafers_produced DECIMAL(18,2),
    wafers_per_hour DECIMAL(18,4),
    running_hours DECIMAL(10,2),
    idle_hours DECIMAL(10,2),
    down_hours DECIMAL(10,2),
    bagged_hours DECIMAL(10,2),
    total_hours DECIMAL(10,2),
    is_bagged BIT DEFAULT 0,
    part_replacement_detected BIT DEFAULT 0,
    counter_column_used VARCHAR(255),
    counter_keyword_used VARCHAR(100),
    calculation_timestamp DATETIME2(7),
    INDEX IX_fact_daily_production_ENTITY_date (ENTITY, production_date),
    INDEX IX_fact_daily_production_FAB_ENTITY (FAB_ENTITY),
    INDEX IX_fact_daily_production_date (production_date),
    INDEX IX_fact_daily_production_FAB (FAB),
    CONSTRAINT UQ_fact_daily_production_ENTITY_date UNIQUE (ENTITY, production_date)
);
GO

PRINT 'Created: dbo.fact_daily_production';
GO

-- fact_weekly_production
DROP TABLE IF EXISTS dbo.fact_weekly_production;
GO

CREATE TABLE dbo.fact_weekly_production (
    weekly_production_id INT IDENTITY(1,1) PRIMARY KEY,
    ENTITY VARCHAR(255) NOT NULL,
    FAB VARCHAR(50) NOT NULL,
    FAB_ENTITY VARCHAR(300) NOT NULL,
    YEARWW VARCHAR(20) NOT NULL,
    total_wafers_produced DECIMAL(18,2),
    total_running_hours DECIMAL(10,2),
    total_idle_hours DECIMAL(10,2),
    total_down_hours DECIMAL(10,2),
    total_bagged_hours DECIMAL(10,2),
    total_hours DECIMAL(10,2),
    avg_wafers_per_hour DECIMAL(18,4),
    part_replacements_count INT DEFAULT 0,
    week_start_date DATE,
    week_end_date DATE,
    days_with_data INT,
    calculation_timestamp DATETIME2(7),
    INDEX IX_fact_weekly_production_ENTITY_WW (ENTITY, YEARWW),
    INDEX IX_fact_weekly_production_FAB_ENTITY (FAB_ENTITY),
    INDEX IX_fact_weekly_production_YEARWW (YEARWW),
    INDEX IX_fact_weekly_production_FAB (FAB),
    CONSTRAINT UQ_fact_weekly_production_ENTITY_WW UNIQUE (ENTITY, YEARWW)
);
GO

PRINT 'Created: dbo.fact_weekly_production';
GO

-- fact_state_hours_daily
DROP TABLE IF EXISTS dbo.fact_state_hours_daily;
GO

CREATE TABLE dbo.fact_state_hours_daily (
    daily_state_hours_id INT IDENTITY(1,1) PRIMARY KEY,
    ENTITY VARCHAR(255) NOT NULL,
    FAB VARCHAR(50) NOT NULL,
    FAB_ENTITY VARCHAR(300) NOT NULL,
    state_date DATE NOT NULL,
    running_hours DECIMAL(10,2) DEFAULT 0,
    idle_hours DECIMAL(10,2) DEFAULT 0,
    down_hours DECIMAL(10,2) DEFAULT 0,
    bagged_hours DECIMAL(10,2) DEFAULT 0,
    total_hours DECIMAL(10,2) DEFAULT 0,
    running_pct DECIMAL(10,4),
    idle_pct DECIMAL(10,4),
    down_pct DECIMAL(10,4),
    is_bagged BIT DEFAULT 0,
    calculation_timestamp DATETIME2(7),
    INDEX IX_fact_state_hours_daily_ENTITY_date (ENTITY, state_date),
    INDEX IX_fact_state_hours_daily_FAB_ENTITY (FAB_ENTITY),
    INDEX IX_fact_state_hours_daily_date (state_date),
    INDEX IX_fact_state_hours_daily_FAB (FAB),
    CONSTRAINT UQ_fact_state_hours_daily_ENTITY_date UNIQUE (ENTITY, state_date)
);
GO

PRINT 'Created: dbo.fact_state_hours_daily';
GO

-- fact_state_hours_weekly
DROP TABLE IF EXISTS dbo.fact_state_hours_weekly;
GO

CREATE TABLE dbo.fact_state_hours_weekly (
    weekly_state_hours_id INT IDENTITY(1,1) PRIMARY KEY,
    ENTITY VARCHAR(255) NOT NULL,
    FAB VARCHAR(50) NOT NULL,
    FAB_ENTITY VARCHAR(300) NOT NULL,
    YEARWW VARCHAR(20) NOT NULL,
    total_running_hours DECIMAL(10,2) DEFAULT 0,
    total_idle_hours DECIMAL(10,2) DEFAULT 0,
    total_down_hours DECIMAL(10,2) DEFAULT 0,
    total_bagged_hours DECIMAL(10,2) DEFAULT 0,
    total_hours DECIMAL(10,2) DEFAULT 0,
    running_pct DECIMAL(10,4),
    idle_pct DECIMAL(10,4),
    down_pct DECIMAL(10,4),
    was_bagged_any_day BIT DEFAULT 0,
    week_start_date DATE,
    week_end_date DATE,
    days_with_data INT,
    calculation_timestamp DATETIME2(7),
    INDEX IX_fact_state_hours_weekly_ENTITY_WW (ENTITY, YEARWW),
    INDEX IX_fact_state_hours_weekly_FAB_ENTITY (FAB_ENTITY),
    INDEX IX_fact_state_hours_weekly_YEARWW (YEARWW),
    INDEX IX_fact_state_hours_weekly_FAB (FAB),
    CONSTRAINT UQ_fact_state_hours_weekly_ENTITY_WW UNIQUE (ENTITY, YEARWW)
);
GO

PRINT 'Created: dbo.fact_state_hours_weekly';
PRINT '';
GO

-- ============================================================================
-- Setup Complete
-- ============================================================================

PRINT '========================================';
PRINT 'Database setup complete!';
PRINT '========================================';
PRINT '';
PRINT 'BRONZE LAYER (2 tables):';
PRINT '  - entity_states_raw';
PRINT '  - counters_raw';
PRINT '';
PRINT 'SILVER LAYER (3 tables):';
PRINT '  - state_hours';
PRINT '  - wafer_production';
PRINT '  - part_replacements';
PRINT '';
PRINT 'GOLD LAYER (4 tables):';
PRINT '  - fact_daily_production';
PRINT '  - fact_weekly_production';
PRINT '  - fact_state_hours_daily';
PRINT '  - fact_state_hours_weekly';
PRINT '';
PRINT 'Total: 9 tables created';
PRINT '';
PRINT 'Next steps:';
PRINT '  1. Run the ETL pipeline to populate Bronze tables';
PRINT '  2. Execute Silver enrichment calculations';
PRINT '  3. Execute Gold aggregations';
PRINT '  4. Connect Power BI to Gold fact tables';
PRINT '========================================';
GO


