"""
Schema Diagnostics
==================
Checks actual data column sizes vs database schema using existing pipeline infrastructure
"""

import pandas as pd
import pyodbc
import yaml
import os
from pathlib import Path
from dotenv import load_dotenv

# Import existing pipeline functions
from utils.helpers import get_recent_work_weeks
from etl.bronze.entity_states_ingestion import EntityStatesIngestion
from utils.database_engine import SQLServerEngine

# Load environment variables
load_dotenv()

def main():
    print("=" * 80)
    print("SCHEMA DIAGNOSTICS")
    print("=" * 80)
    
    # Load config
    config_path = Path('config/config.yaml')
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # 1. Use pipeline's file discovery to find EntityStates file
    print("\n" + "=" * 80)
    print("STEP 1: Discovering EntityStates File")
    print("=" * 80)
    
    ingestion = EntityStatesIngestion(config)
    files_to_process = ingestion.discover_files(mode='incremental')
    
    if not files_to_process:
        print("ERROR: No EntityStates files found")
        return
    
    ww_str, file_path = files_to_process[0]
    print(f"\nFound file: {file_path}")
    print(f"Work week: {ww_str}")
    
    # 2. Load and check CSV column max lengths
    print("\n" + "=" * 80)
    print("STEP 2: CSV File Column Max Lengths")
    print("=" * 80)
    
    print(f"\nReading: {file_path}")
    df = pd.read_csv(file_path)
    print(f"Loaded: {len(df)} rows, {len(df.columns)} columns\n")
    
    csv_max_lengths = {}
    print("Column Max Lengths (string columns only):")
    for col in df.columns:
        if df[col].dtype == 'object':
            max_len = df[col].astype(str).str.len().max()
            csv_max_lengths[col] = max_len
            print(f"  {col:30} {max_len:>5} chars")
    
    # 3. Check database schema using existing engine
    print("\n" + "=" * 80)
    print("STEP 3: Database Table Schema")
    print("=" * 80)
    
    engine = SQLServerEngine(config, 'ENTITY_STATES_SQLSERVER_OUTPUT')
    print(f"\nConnecting to: {engine.server}/{engine.database}")
    
    conn = engine.get_connection()
    cursor = conn.cursor()
    
    cursor.execute(f"""
        SELECT 
            COLUMN_NAME, 
            DATA_TYPE, 
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{engine.table_name}' 
        ORDER BY ORDINAL_POSITION
    """)
    
    print(f"\n{engine.table_name} columns:")
    print(f"  {'Column Name':30} {'Type':40}")
    print("  " + "-" * 72)
    
    db_varchar_sizes = {}
    for row in cursor.fetchall():
        col_name = row.COLUMN_NAME
        data_type = row.DATA_TYPE
        
        if row.CHARACTER_MAXIMUM_LENGTH:
            type_detail = f"{data_type}({row.CHARACTER_MAXIMUM_LENGTH})"
            db_varchar_sizes[col_name] = row.CHARACTER_MAXIMUM_LENGTH
        elif row.NUMERIC_PRECISION:
            type_detail = f"{data_type}({row.NUMERIC_PRECISION},{row.NUMERIC_SCALE})"
        else:
            type_detail = data_type
        
        print(f"  {col_name:30} {type_detail:>40}")
    
    cursor.close()
    conn.close()
    
    # 4. Compare and identify issues
    print("\n" + "=" * 80)
    print("STEP 4: Identifying Schema Mismatches")
    print("=" * 80)
    
    issues_found = False
    print("\nChecking for columns where data exceeds database size:\n")
    
    # Check CSV columns against DB
    for col in df.columns:
        if col in csv_max_lengths and col in db_varchar_sizes:
            max_data = csv_max_lengths[col]
            max_db = db_varchar_sizes[col]
            
            if max_data > max_db:
                print(f"  ISSUE: {col}")
                print(f"    Data max length: {max_data} chars")
                print(f"    Database size:   VARCHAR({max_db})")
                print(f"    RECOMMENDED:     VARCHAR({max_data + 50})")
                print()
                issues_found = True
    
    # Check for metadata columns that will be added by pipeline
    metadata_cols = ['source_file', 'load_ww']
    print("Checking metadata columns:")
    for meta_col in metadata_cols:
        if meta_col in db_varchar_sizes:
            db_size = db_varchar_sizes[meta_col]
            
            # Estimate needed size
            if meta_col == 'source_file':
                needed = len(str(file_path))
                print(f"  {meta_col}: Database={db_size}, Actual path length={needed}")
                if needed > db_size:
                    print(f"    ISSUE: File path too long!")
                    print(f"    RECOMMENDED: VARCHAR({needed + 100})")
                    issues_found = True
            elif meta_col == 'load_ww':
                needed = len(ww_str)
                print(f"  {meta_col}: Database={db_size}, Actual WW length={needed}")
                if needed > db_size:
                    print(f"    ISSUE: Work week string too long!")
                    print(f"    RECOMMENDED: VARCHAR({needed + 10})")
                    issues_found = True
    
    print()
    if not issues_found:
        print("  ✓ No issues found - all data fits in database columns")
    else:
        print("  ✗ Issues found - update setup_database.sql with recommended sizes")
    
    print("\n" + "=" * 80)
    print("DIAGNOSTICS COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    main()
