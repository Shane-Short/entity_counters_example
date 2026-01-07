# STEP 3: Track part replacements
    logger.info("STEP 3: Tracking part replacements")
    replacements_df = track_part_replacements(config, production_df)
    
    # Initialize as empty DataFrame in case of issues
    if replacements_df is None:
        replacements_df = pd.DataFrame()
    
    if not replacements_df.empty:
        logger.info(f"Part replacements tracked: {len(replacements_df)} events")
        
        # FIX: Convert replacement_date to proper date format (remove timezone, ensure DATE not DATETIME)
        if 'replacement_date' in replacements_df.columns:
            replacements_df['replacement_date'] = pd.to_datetime(replacements_df['replacement_date']).dt.date
        
        # FIX: Convert replacement_detected_ts to timezone-naive datetime
        if 'replacement_detected_ts' in replacements_df.columns:
            replacements_df['replacement_detected_ts'] = pd.to_datetime(replacements_df['replacement_detected_ts'])
            if replacements_df['replacement_detected_ts'].dt.tz is not None:
                replacements_df['replacement_detected_ts'] = replacements_df['replacement_detected_ts'].dt.tz_localize(None)
        
        # DEBUG: Check data types AFTER conversion
        logger.info("Part replacements DataFrame dtypes (after conversion):")
        logger.info(f"\n{replacements_df.dtypes}")
        
        rows_loaded = load_to_sqlserver(
            replacements_df,
            config,
            'PART_REPLACEMENTS_SQLSERVER_OUTPUT',
            if_exists='append'
        )
        logger.info(f"Part replacements: {rows_loaded} rows loaded")
    else:
        logger.info("No part replacements detected")
