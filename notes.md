def aggregate_by_state(self, df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate hours by entity, date, and individual state.
    """
    # Extract date from DAY_SHIFT (format: "12/31 - S7")
    date_str = df['DAY_SHIFT'].str.split(' - ').str[0]
    
    # Add year from load_date column
    year = pd.to_datetime(df['load_date']).dt.year
    df['state_date'] = pd.to_datetime(year.astype(str) + '/' + date_str).dt.date
    
    # Group by ENTITY, state_date, and actual ENTITY_STATE - SUM to combine duplicates
    state_detail = df.groupby(['ENTITY', 'FAB', 'state_date', 'ENTITY_STATE'], as_index=False).agg({
        'HOURS_IN_STATE': 'sum'
    })
    
    state_detail = state_detail.rename(columns={'HOURS_IN_STATE': 'hours', 'ENTITY_STATE': 'state_name'})
    
    # Add state category for each state
    state_detail['state_category'] = state_detail['state_name'].apply(self.classify_state)
    
    # Add calculation timestamp
    state_detail['calculation_timestamp'] = datetime.now()
    
    # CRITICAL: Remove any remaining duplicates (in case of data quality issues)
    before_dedup = len(state_detail)
    state_detail = state_detail.drop_duplicates(subset=['ENTITY', 'state_date', 'state_name'], keep='first')
    after_dedup = len(state_detail)
    
    if before_dedup > after_dedup:
        logger.warning(f"Removed {before_dedup - after_dedup} duplicate state_hours_detail rows after aggregation")
    
    logger.info(f"Detailed state tracking: {len(state_detail)} entity-date-state combinations")
    logger.info(f"Unique states found: {state_detail['state_name'].nunique()}")
    
    unique_states = state_detail['state_name'].unique()
    logger.info(f"State breakdown: {', '.join(unique_states[:20])}")
    
    return state_detail
