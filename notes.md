def create_gold_facts(
    config: Dict,
    production_df: pd.DataFrame,
    state_hours_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Standalone function to create Gold layer facts.
    Aggregates data AND loads to database.
    """
    from utils.database_engine import load_to_sqlserver
    
    aggregator = GoldAggregations(config)

    # Create all aggregations
    daily_production, weekly_production, daily_state_hours, weekly_state_hours = aggregator.create_all_facts(
        production_df,
        state_hours_df,
    )
    
    # Load to SQL Server
    logger.info("Loading Gold layer tables to SQL Server")
    
    if not daily_production.empty:
        rows_loaded = load_to_sqlserver(
            daily_production, 
            config, 
            table_params_key='DAILY_PRODUCTION_SQLSERVER_OUTPUT', 
            if_exists='append'
        )
        logger.info(f"Loaded {rows_loaded} rows to fact_daily_production")
    else:
        logger.warning("Daily production fact table is empty")
    
    if not weekly_production.empty:
        rows_loaded = load_to_sqlserver(
            weekly_production, 
            config, 
            table_params_key='WEEKLY_PRODUCTION_SQLSERVER_OUTPUT', 
            if_exists='append'
        )
        logger.info(f"Loaded {rows_loaded} rows to fact_weekly_production")
    else:
        logger.warning("Weekly production fact table is empty")
    
    if not daily_state_hours.empty:
        rows_loaded = load_to_sqlserver(
            daily_state_hours, 
            config, 
            table_params_key='DAILY_STATE_HOURS_SQLSERVER_OUTPUT', 
            if_exists='append'
        )
        logger.info(f"Loaded {rows_loaded} rows to fact_state_hours_daily")
    else:
        logger.warning("Daily state hours fact table is empty")
    
    if not weekly_state_hours.empty:
        rows_loaded = load_to_sqlserver(
            weekly_state_hours, 
            config, 
            table_params_key='WEEKLY_STATE_HOURS_SQLSERVER_OUTPUT', 
            if_exists='append'
        )
        logger.info(f"Loaded {rows_loaded} rows to fact_state_hours_weekly")
    else:
        logger.warning("Weekly state hours fact table is empty")
    
    logger.info("Gold layer database loading complete")

    return daily_production, weekly_production, daily_state_hours, weekly_state_hours
