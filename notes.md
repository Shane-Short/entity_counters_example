# STEP 3: Track part replacements
    logger.info("STEP 3: Tracking part replacements")
    replacements_df = track_part_replacements(config, production_df)
    
    # Initialize as empty DataFrame in case of issues
    if replacements_df is None:
        replacements_df = pd.DataFrame()
    
    if not replacements_df.empty:
        logger.info(f"Part replacements tracked: {len(replacements_df)} events")
        
        # DEBUG: Check data types
        logger.info("Part replacements DataFrame dtypes:")
        logger.info(f"\n{replacements_df.dtypes}")
        
        # DEBUG: Check for problematic values
        for col in replacements_df.columns:
            if replacements_df[col].dtype == 'object':
                logger.info(f"Column {col} sample values: {replacements_df[col].head(3).tolist()}")
        
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



