def run_silver_enrichment(config: Dict, entity_states_df: pd.DataFrame, counters_df: pd.DataFrame, mode: str = 'full'):
    """
    Run Silver layer enrichment.
    """
    from utils.database_engine import load_to_sqlserver
    
    logger.info("")
    logger.info("=" * 80)
    logger.info(f"SILVER LAYER - {mode.upper()} MODE")
    logger.info("=" * 80)
    
    # STEP 1: Calculate state hours
    logger.info("STEP 1: Calculating state hours")
    state_hours_df = calculate_state_hours(config, entity_states_df)
    
    if not state_hours_df.empty:
        logger.info(f"State hours complete: {len(state_hours_df)} rows")
        rows_loaded = load_to_sqlserver(
            state_hours_df,
            config,
            'STATE_HOURS_SQLSERVER_OUTPUT',
            if_exists='append'
        )
        logger.info(f"State hours: {rows_loaded} rows loaded to SQL Server")
    
    # STEP 1B: Calculate detailed state hours
    logger.info("STEP 1B: Calculating detailed state hours")
    calculator = StateHoursCalculator(config)
    state_hours_detail_df = calculator.aggregate_by_state(entity_states_df)
    
    if not state_hours_detail_df.empty:
        logger.info(f"State hours detail complete: {len(state_hours_detail_df)} rows")
        rows_loaded = load_to_sqlserver(
            state_hours_detail_df,
            config,
            'STATE_HOURS_DETAIL_SQLSERVER_OUTPUT',
            if_exists='append'
        )
        logger.info(f"State hours detail: {rows_loaded} rows loaded")
    
    # STEP 2: Calculate wafer production
    logger.info("STEP 2: Calculating wafer production")
    production_df = calculate_wafer_production(config, counters_df, state_hours_df)
    
    if not production_df.empty:
        logger.info(f"Wafer production complete: {len(production_df)} rows")
        rows_loaded = load_to_sqlserver(
            production_df,
            config,
            'WAFER_PRODUCTION_SQLSERVER_OUTPUT',
            if_exists='append'
        )
        logger.info(f"Wafer production: {rows_loaded} rows loaded")
    
    # STEP 3: Track part replacements
    logger.info("STEP 3: Tracking part replacements")
    replacements_df = track_part_replacements(config, production_df)
    
    if not replacements_df.empty:
        logger.info(f"Part replacements tracked: {len(replacements_df)} events")
        rows_loaded = load_to_sqlserver(
            replacements_df,
            config,
            'PART_REPLACEMENTS_SQLSERVER_OUTPUT',
            if_exists='append'
        )
        logger.info(f"Part replacements: {rows_loaded} rows loaded")
    else:
        logger.info("No part replacements detected")
    
    logger.info("SILVER LAYER COMPLETE")
    
    return state_hours_df, production_df, replacements_df
