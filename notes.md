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
    
    # Load and substitute environment variables
    import os
    from dotenv import load_dotenv
    import re
    
    load_dotenv()
    
    with open(config_path, 'r') as f:
        config_text = f.read()
    
    # Substitute environment variables
    def replace_env_vars(match):
        var_name = match.group(1)
        return os.getenv(var_name, match.group(0))
    
    config_text = re.sub(r'\$\{([^}]+)\}', replace_env_vars, config_text)
    config = yaml.safe_load(config_text)
    
    # Get runtime environment from config
    runtime_env = config['runtime']['runtime_env']
    print(f"Using runtime environment: {runtime_env}")
    
    # Get environment config
    env_config = config['environments'][runtime_env]
    
    # Use SQLServerEngine to get connection (reuse existing logic)
    print("\nConnecting to SQL Server...")
    
    # Create a minimal config for SQLServerEngine
    temp_config = {
        'table_parameters': {
            'TEMP': {
                'sqlserver': env_config
            }
        }
    }
    
    from utils.database_engine import SQLServerEngine
    engine = SQLServerEngine(temp_config, 'TEMP')
    conn = engine.get_connection()
    print("  Connected successfully!")
