# STEP 1B: Calculate detailed state hours
    logger.info("STEP 1B: Calculating detailed state hours")
    calculator = StateHoursCalculator(config)
    state_hours_detail_df = calculator.aggregate_by_state(entity_states_df)
    
    # CRITICAL: Validate no duplicates exist before attempting insert
    duplicate_check = state_hours_detail_df.groupby(['ENTITY', 'state_date', 'state_name']).size()
    duplicates = duplicate_check[duplicate_check > 1]
    
    if len(duplicates) > 0:
        logger.error("=" * 80)
        logger.error("DUPLICATE KEYS DETECTED IN STATE_HOURS_DETAIL!")
        logger.error("=" * 80)
        logger.error(f"Found {len(duplicates)} duplicate entity-date-state combinations:")
        logger.error(f"\n{duplicates.head(20)}")
        logger.error("=" * 80)
        logger.error("Stopping pipeline - fix duplicates before loading")
        raise ValueError(f"Cannot load state_hours_detail: {len(duplicates)} duplicate keys exist")
    
    if not state_hours_detail_df.empty:
        logger.info(f"State hours detail complete: {len(state_hours_detail_df)} rows")
        rows_loaded = load_to_sqlserver(
            state_hours_detail_df,
            config,
            'STATE_HOURS_DETAIL_SQLSERVER_OUTPUT',
            if_exists='append'
        )
        logger.info(f"State hours detail: {rows_loaded} rows loaded")
