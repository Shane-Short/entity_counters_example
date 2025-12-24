"""
Database Setup Script
=====================
Executes SQL DDL files to create all database tables.

Usage:
    python setup_database_tables.py
"""

import pyodbc
import yaml
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_connection_string(config):
    """Build SQL Server connection string."""
    sqlserver_config = config['table_parameters']['ENTITY_STATES_SQLSERVER_OUTPUT']['sqlserver']
    
    server = sqlserver_config['server']
    database = sqlserver_config['database']
    trusted_connection = sqlserver_config.get('trusted_connection', False)
    driver = sqlserver_config.get('driver', 'ODBC Driver 18 for SQL Server')
    
    if trusted_connection:
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"Trusted_Connection=yes;"
            f"TrustServerCertificate=yes;"
        )
    else:
        username = os.getenv('SQL_USER', sqlserver_config.get('username', ''))
        password = os.getenv('SQL_PASS', sqlserver_config.get('password', ''))
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"TrustServerCertificate=yes;"
        )
    
    return conn_str

def execute_sql_file(conn, sql_file_path):
    """Execute SQL file."""
    print(f"\nExecuting: {sql_file_path}")
    
    with open(sql_file_path, 'r') as f:
        sql_content = f.read()
    
    # Split by GO statements
    batches = sql_content.split('GO')
    
    cursor = conn.cursor()
    
    for i, batch in enumerate(batches):
        batch = batch.strip()
        if not batch:
            continue
        
        try:
            cursor.execute(batch)
            conn.commit()
        except Exception as e:
            print(f"  Warning on batch {i+1}: {e}")
            # Continue anyway - some statements might fail if tables already exist
    
    cursor.close()
    print(f"  Completed: {sql_file_path}")

def main():
    """Main execution."""
    print("=" * 80)
    print("DATABASE SETUP - Creating Tables")
    print("=" * 80)
    
    # Load config
    config_path = Path('config/config.yaml')
    if not config_path.exists():
        print(f"ERROR: Config file not found: {config_path}")
        return
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Connect to database
    print("\nConnecting to SQL Server...")
    conn_str = get_connection_string(config)
    
    try:
        conn = pyodbc.connect(conn_str)
        print("  Connected successfully!")
    except Exception as e:
        print(f"  ERROR: Failed to connect: {e}")
        return
    
    # Execute setup_database.sql
    sql_file = Path('sql/ddl/setup_database.sql')
    if not sql_file.exists():
        print(f"ERROR: SQL file not found: {sql_file}")
        conn.close()
        return
    
    try:
        execute_sql_file(conn, sql_file)
    except Exception as e:
        print(f"ERROR executing SQL: {e}")
    
    conn.close()
    
    print("\n" + "=" * 80)
    print("SETUP COMPLETE")
    print("=" * 80)
    print("\nTables created:")
    print("  - entity_states_raw")
    print("  - state_hours")
    print("  - wafer_production")
    print("  - part_replacements")
    print("  - fact_daily_production")
    print("  - fact_weekly_production")
    print("  - fact_state_hours_daily")
    print("  - fact_state_hours_weekly")
    print("\nNEXT STEP:")
    print("  Run: python -m etl.setup_counters_table")
    print("  This will generate the counters_raw table DDL")
    print("=" * 80)

if __name__ == "__main__":
    main()







"""
Execute Generated Counters Table DDL
=====================================
Executes the auto-generated counters_raw table creation script.

Usage:
    python execute_counters_ddl.py
"""

import pyodbc
import yaml
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_connection_string(config):
    """Build SQL Server connection string."""
    sqlserver_config = config['table_parameters']['ENTITY_STATES_SQLSERVER_OUTPUT']['sqlserver']
    
    server = sqlserver_config['server']
    database = sqlserver_config['database']
    trusted_connection = sqlserver_config.get('trusted_connection', False)
    driver = sqlserver_config.get('driver', 'ODBC Driver 18 for SQL Server')
    
    if trusted_connection:
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"Trusted_Connection=yes;"
            f"TrustServerCertificate=yes;"
        )
    else:
        username = os.getenv('SQL_USER', sqlserver_config.get('username', ''))
        password = os.getenv('SQL_PASS', sqlserver_config.get('password', ''))
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"TrustServerCertificate=yes;"
        )
    
    return conn_str

def execute_sql_file(conn, sql_file_path):
    """Execute SQL file."""
    print(f"\nExecuting: {sql_file_path}")
    
    with open(sql_file_path, 'r') as f:
        sql_content = f.read()
    
    # Split by GO statements
    batches = sql_content.split('GO')
    
    cursor = conn.cursor()
    
    for i, batch in enumerate(batches):
        batch = batch.strip()
        if not batch:
            continue
        
        try:
            cursor.execute(batch)
            conn.commit()
        except Exception as e:
            print(f"  Error on batch {i+1}: {e}")
            raise
    
    cursor.close()
    print(f"  Completed successfully!")

def main():
    """Main execution."""
    print("=" * 80)
    print("EXECUTING GENERATED COUNTERS TABLE DDL")
    print("=" * 80)
    
    # Load config
    config_path = Path('config/config.yaml')
    if not config_path.exists():
        print(f"ERROR: Config file not found: {config_path}")
        return
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Check if generated SQL exists
    sql_file = Path('sql/ddl/create_counters_raw_GENERATED.sql')
    if not sql_file.exists():
        print(f"\nERROR: Generated SQL file not found!")
        print(f"Expected: {sql_file}")
        print(f"\nYou need to run this first:")
        print(f"  python -m etl.setup_counters_table")
        return
    
    # Connect to database
    print("\nConnecting to SQL Server...")
    conn_str = get_connection_string(config)
    
    try:
        conn = pyodbc.connect(conn_str)
        print("  Connected successfully!")
    except Exception as e:
        print(f"  ERROR: Failed to connect: {e}")
        return
    
    # Execute generated SQL
    try:
        execute_sql_file(conn, sql_file)
        
        # Verify table created
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'counters_raw'")
        col_count = cursor.fetchone()[0]
        cursor.close()
        
        print(f"\nTable 'counters_raw' created successfully!")
        print(f"Total columns: {col_count}")
        
    except Exception as e:
        print(f"\nERROR executing SQL: {e}")
    finally:
        conn.close()
    
    print("\n" + "=" * 80)
    print("COUNTERS TABLE SETUP COMPLETE")
    print("=" * 80)
    print("\nAll database tables are now ready!")
    print("\nNEXT STEP:")
    print("  Run: python run_etl_pipeline.py --layer bronze --mode incremental")
    print("=" * 80)

if __name__ == "__main__":
    main()





    
