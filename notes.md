    config_text = re.sub(r'\$\{([^}]+)\}', replace_env_vars, config_text)
    config = yaml.safe_load(config_text)
    
    # Connect using any table config (they all point to same database)
    # We use ENTITY_STATES just as a convention (first table alphabetically)
    print("\nConnecting to SQL Server...")
    engine = SQLServerEngine(config, 'ENTITY_STATES_SQLSERVER_OUTPUT')
    conn = engine.get_connection()
    print("  Connected successfully!")
    
