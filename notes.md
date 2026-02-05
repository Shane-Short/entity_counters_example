def delete_dates_from_table(config: Dict, table_name: str, dates: list, date_column: str):
    """Delete specific dates from a table before re-inserting."""
    if not dates:
        return
    
    from utils.database_engine import get_database_connection
    
    # Convert dates to strings for SQL
    date_strings = [str(d) for d in dates]
    date_list = "','".join(date_strings)
    delete_query = f"DELETE FROM dbo.{table_name} WHERE {date_column} IN ('{date_list}')"
    
    conn = get_database_connection(config)
    cursor = conn.cursor()
    cursor.execute(delete_query)
    conn.commit()
    cursor.close()
    conn.close()
    
    logger.info(f"Deleted {len(dates)} dates from {table_name}")




    def create_gold_facts(
    config: Dict,
    production_df: pd.DataFrame,
    state_hours_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Standalone function to create Gold layer facts.
    Aggregates data AND loads to database.
    """
    aggregator = GoldAggregations(config)
    
    (
        daily_production,
        weekly_production,
        daily_state_hours,
        weekly_state_hours,
    ) = aggregator.create_all_facts(
        production_df,
        state_hours_df,
    )

    logger.info("Loading Gold layer tables to SQL Server")

    # === DAILY PRODUCTION ===
    if not daily_production.empty:
        dates_to_refresh = daily_production['production_date'].unique().tolist()
        delete_dates_from_table(config, "fact_daily_production", dates_to_refresh, date_column="production_date")
        
        rows_loaded = load_to_sqlserver(
            daily_production,
            config,
            table_params_key="DAILY_PRODUCTION_SQLSERVER_OUTPUT",
            if_exists="append",
        )
        logger.info(f"Loaded {rows_loaded} rows to fact_daily_production")
    else:
        logger.warning("Daily production fact table is empty")

    # === WEEKLY PRODUCTION ===
    if not weekly_production.empty:
        weeks_to_refresh = weekly_production['YEARWW'].unique().tolist()
        delete_weeks_from_table(config, "fact_weekly_production", weeks_to_refresh)
        
        rows_loaded = load_to_sqlserver(
            weekly_production,
            config,
            table_params_key="WEEKLY_PRODUCTION_SQLSERVER_OUTPUT",
            if_exists="append",
        )
        logger.info(f"Loaded {rows_loaded} rows to fact_weekly_production")
    else:
        logger.warning("Weekly production fact table is empty")

    # === DAILY STATE HOURS ===
    if not daily_state_hours.empty:
        dates_to_refresh = daily_state_hours['state_date'].unique().tolist()
        delete_dates_from_table(config, "fact_state_hours_daily", dates_to_refresh, date_column="state_date")
        
        rows_loaded = load_to_sqlserver(
            daily_state_hours,
            config,
            table_params_key="DAILY_STATE_HOURS_SQLSERVER_OUTPUT",
            if_exists="append",
        )
        logger.info(f"Loaded {rows_loaded} rows to fact_state_hours_daily")
    else:
        logger.warning("Daily state hours fact table is empty")

    # === WEEKLY STATE HOURS ===
    if not weekly_state_hours.empty:
        weeks_to_refresh = weekly_state_hours['YEARWW'].unique().tolist()
        delete_weeks_from_table(config, "fact_state_hours_weekly", weeks_to_refresh)
        
        rows_loaded = load_to_sqlserver(
            weekly_state_hours,
            config,
            table_params_key="WEEKLY_STATE_HOURS_SQLSERVER_OUTPUT",
            if_exists="append",
        )
        logger.info(f"Loaded {rows_loaded} rows to fact_state_hours_weekly")
    else:
        logger.warning("Weekly state hours fact table is empty")

    logger.info("Gold layer database loading complete")
    
    return (
        daily_production,
        weekly_production,
        daily_state_hours,
        weekly_state_hours,
    )
