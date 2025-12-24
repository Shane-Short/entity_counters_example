def main():
    """Main execution."""
    print("=" * 80)
    print("DATABASE SETUP - Creating ALL Tables")
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
    
    # STEP 1: Execute setup_database.sql (creates 8 tables)
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
        return
    
    print("\n" + "=" * 80)
    print("Step 1/2: Base tables created (8 tables)")
    print("=" * 80)
    
    # STEP 2: Generate and execute counters_raw DDL
    print("\n" + "=" * 80)
    print("Step 2/2: Generating counters_raw table...")
    print("=" * 80)
    
    try:
        # Import the setup_counters_table module
        from etl.setup_counters_table import CountersTableSetup
        
        # Generate the DDL
        setup = CountersTableSetup(config_path='config/config.yaml')
        sample_file = setup.find_sample_counters_file()
        csv_columns = setup.read_column_names(sample_file)
        generated_sql_path = setup.generate_sql_script(csv_columns)
        
        print(f"\nGenerated DDL: {generated_sql_path}")
        print(f"Now executing to create counters_raw table...")
        
        # Execute the generated SQL
        execute_sql_file(conn, generated_sql_path)
        
        # Verify table created
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'counters_raw'")
        col_count = cursor.fetchone()[0]
        cursor.close()
        
        print(f"\nTable 'counters_raw' created successfully!")
        print(f"Total columns: {col_count}")
        
    except ImportError as e:
        print(f"\nERROR: Could not import setup_counters_table module: {e}")
        print("Make sure etl/__init__.py exists")
    except Exception as e:
        print(f"\nERROR creating counters_raw table: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()
    
    print("\n" + "=" * 80)
    print("DATABASE SETUP COMPLETE")
    print("=" * 80)
    print("\nAll tables created:")
    print("  BRONZE LAYER:")
    print("    - entity_states_raw")
    print("    - counters_raw")
    print("  SILVER LAYER:")
    print("    - state_hours")
    print("    - wafer_production")
    print("    - part_replacements")
    print("  GOLD LAYER:")
    print("    - fact_daily_production")
    print("    - fact_weekly_production")
    print("    - fact_state_hours_daily")
    print("    - fact_state_hours_weekly")
    print("\nTotal: 9 tables")
    print("\nNEXT STEP:")
    print("  Run: python run_etl_pipeline.py --layer bronze --mode incremental")
    print("=" * 80)
