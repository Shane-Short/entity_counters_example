def fix_disconnected_counter_rows(counters_df: pd.DataFrame) -> pd.DataFrame:
    """
    Detects disconnected rows in counter data and forward-fills with last valid values.
    
    A row is considered "disconnected" if ALL counter value columns are either:
    - NULL/NaN, OR
    - Between -5 and 5 (essentially zero or near-zero noise)
    
    For disconnected rows:
    - All counter values are replaced with the last valid values for that FAB_ENTITY
    - is_disconnected flag is set to True
    
    Parameters
    ----------
    counters_df : pd.DataFrame
        Raw counters data from Bronze layer
        
    Returns
    -------
    pd.DataFrame
        Cleaned counters data with is_disconnected flag added
    """
    import logging
    logger = logging.getLogger(__name__)
    
    logger.info("Checking for disconnected counter rows...")
    
    # Make a copy to avoid modifying original
    df = counters_df.copy()
    
    # Identify non-counter columns (metadata columns to exclude from the check)
    # These are columns that are NOT counter values
    metadata_columns = [
        'FAB', 'ENTITY', 'FAB_ENTITY', 'DATE', 'Date', 'date',
        'YEARWW', 'YearWW', 'yearww', 'FACILITY', 'Facility',
        'CEID', 'Ceid', 'TOOLSET', 'Toolset'
    ]
    
    # Get counter columns (all columns except metadata)
    counter_columns = [
        col for col in df.columns 
        if col not in metadata_columns and col.upper() not in [m.upper() for m in metadata_columns]
    ]
    
    logger.info(f"Found {len(counter_columns)} counter columns to check")
    
    # Function to check if a row is disconnected
    def is_row_disconnected(row):
        """Check if all counter values in row are NULL or between -5 and 5"""
        for col in counter_columns:
            val = row[col]
            # If value is not null AND outside -5 to 5 range, row is NOT disconnected
            if pd.notna(val) and (val < -5 or val > 5):
                return False
        # All values were null or between -5 and 5
        return True
    
    # Identify disconnected rows
    df['is_disconnected'] = df.apply(is_row_disconnected, axis=1)
    
    disconnected_count = df['is_disconnected'].sum()
    logger.info(f"Found {disconnected_count} disconnected rows out of {len(df)} total")
    
    if disconnected_count == 0:
        logger.info("No disconnected rows to fix")
        return df
    
    # Sort by FAB_ENTITY and date to ensure proper forward-fill order
    date_col = None
    for col in ['DATE', 'Date', 'date']:
        if col in df.columns:
            date_col = col
            break
    
    if date_col is None:
        logger.error("Could not find date column in counters data")
        raise ValueError("Date column not found in counters_df")
    
    # Determine FAB_ENTITY column
    fab_entity_col = None
    for col in ['FAB_ENTITY', 'Fab_Entity', 'fab_entity']:
        if col in df.columns:
            fab_entity_col = col
            break
    
    if fab_entity_col is None:
        logger.error("Could not find FAB_ENTITY column in counters data")
        raise ValueError("FAB_ENTITY column not found in counters_df")
    
    logger.info(f"Using '{fab_entity_col}' for entity grouping and '{date_col}' for date sorting")
    
    # Sort for proper forward-fill
    df = df.sort_values([fab_entity_col, date_col]).reset_index(drop=True)
    
    # Forward-fill counter values within each FAB_ENTITY group
    # Only fill disconnected rows with values from last valid row
    logger.info("Forward-filling disconnected rows with last valid values...")
    
    for fab_entity in df[fab_entity_col].unique():
        entity_mask = df[fab_entity_col] == fab_entity
        entity_indices = df[entity_mask].index.tolist()
        
        last_valid_values = {}
        
        for idx in entity_indices:
            if df.loc[idx, 'is_disconnected']:
                # This row is disconnected - fill with last valid values
                if last_valid_values:
                    for col in counter_columns:
                        if col in last_valid_values:
                            df.loc[idx, col] = last_valid_values[col]
            else:
                # This row is valid - store its values
                for col in counter_columns:
                    val = df.loc[idx, col]
                    if pd.notna(val):
                        last_valid_values[col] = val
    
    logger.info(f"Forward-fill complete. Fixed {disconnected_count} disconnected rows.")
    
    return df
