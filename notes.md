def main(full_refresh=False):
    """Main execution."""
    print("=" * 80)
    print("DATABASE SETUP - Creating ALL Tables")
    if full_refresh:
        print("MODE: FULL REFRESH (dropping existing tables)")
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
    
    # FULL REFRESH: Drop all tables
    if full_refresh:
        print("\n" + "=" * 80)
        print("FULL REFRESH: Dropping existing tables")
        print("=" * 80)
        
        tables_to_drop = [
            'fact_state_hours_weekly',
            'fact_state_hours_daily',
            'fact_weekly_production',
            'fact_daily_production',
            'part_replacements',
            'wafer_production',
            'state_hours',
            'counters_raw',
            'entity_states_raw'
        ]
        
        cursor = conn.cursor()
        for table in tables_to_drop:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS dbo.{table}")
                conn.commit()
                print(f"  Dropped: {table}")
            except Exception as e:
                print(f"  Warning dropping {table}: {e}")
        cursor.close()
        
        print("  All tables dropped")
    
    # STEP 1: Execute setup_database.sql (creates 8 tables)
    # ... rest of existing code ...
