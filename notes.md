# STEP 3: Track part replacements
    logger.info("STEP 3: Tracking part replacements")
    print("\nSTEP 3: Starting part replacement tracking...")
    
    # DEBUG: Check production_df
    part_repl_count = production_df['part_replacement_detected'].sum()
    print(f"  production_df has {len(production_df)} rows")
    print(f"  {part_repl_count} rows have part_replacement_detected=True")
    
    if part_repl_count > 0:
        sample_row = production_df[production_df['part_replacement_detected'] == True].iloc[0]
        print(f"  Sample all_part_replacements value: {sample_row['all_part_replacements']}")
        print(f"  Type: {type(sample_row['all_part_replacements'])}")
    
    replacements_df = track_part_replacements(config, production_df)
    
    print(f"  Part replacements returned: {len(replacements_df) if replacements_df is not None else 0} rows")
    
    if replacements_df is None or replacements_df.empty:
        logger.warning("No part replacements to load")
        print("  WARNING: No part replacements extracted!")
    else:
        print(f"  ✓ Successfully extracted {len(replacements_df)} part replacement events")
        # Load to database
        load_to_sqlserver(replacements_df, config, 'PART_REPLACEMENTS_SQLSERVER_OUTPUT', if_exists='append')
