def get_database_connection(config: Dict, environment: str = None) -> pyodbc.Connection:
    """
    Get generic database connection without table-specific information.
    Uses environment specified in runtime.runtime_env if not provided.
    
    Parameters
    ----------
    config : dict
        Configuration dictionary
    environment : str, optional
        Environment name. If None, uses config['runtime']['runtime_env']
    
    Returns
    -------
    pyodbc.Connection
        Database connection
    """
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    # Get environment
    if environment is None:
        environment = config['runtime']['runtime_env']
    
    # Get environment config
    env_config = config['environments'][environment]['sqlserver']
    
    # Get values with environment variable substitution
    server = os.getenv('SQL_SERVER', env_config.get('server', ''))
    port = os.getenv('SQL_PORT', env_config.get('port', '1433'))
    username = os.getenv('SQL_USERNAME', env_config.get('username', ''))
    password = os.getenv('SQL_PASSWORD', env_config.get('password', ''))
    driver = env_config.get('driver', 'ODBC Driver 18 for SQL Server')
    database = env_config['database']
    
    # Build server string (no tcp: prefix for instance names)
    if '\\' in server:
        server_str = server  # Instance name format
    elif ',' in server:
        server_str = server  # Port already specified
    else:
        server_str = f"{server},{port}" if port else server
    
    # Build connection string
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server_str};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"TrustServerCertificate=yes;"
    )
    
    logger.debug(f"Connecting to {server_str}/{database}")
    return pyodbc.connect(conn_str, timeout=30)
