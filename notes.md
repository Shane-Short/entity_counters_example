# CRITICAL: Remove any remaining duplicates (in case of data quality issues)
    before_dedup = len(state_detail)
    state_detail = state_detail.drop_duplicates(subset=['ENTITY', 'state_date', 'state_name'], keep='first')
    after_dedup = len(state_detail)
    
    if before_dedup > after_dedup:
        logger.warning(f"Removed {before_dedup - after_dedup} duplicate state_hours_detail rows after aggregation")
    
    # DEBUG: Check for any remaining duplicates
    duplicate_check = state_detail.groupby(['ENTITY', 'state_date', 'state_name']).size()
    if (duplicate_check > 1).any():
        logger.error("DUPLICATES STILL EXIST AFTER drop_duplicates!")
        dupes = duplicate_check[duplicate_check > 1]
        logger.error(f"Found {len(dupes)} duplicate combinations:")
        logger.error(f"{dupes.head(10)}")
    
    logger.info(f"Detailed state tracking: {len(state_detail)} entity-date-state combinations")
    logger.info(f"Unique states found: {state_detail['state_name'].nunique()}")
    
    unique_states = state_detail['state_name'].unique()
    logger.info(f"State breakdown: {', '.join(unique_states[:20])}")
    
    return state_detail


