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
    import numpy as np
    logger = logging.getLogger(__name__)
    
    logger.info("Checking for disconnected counter rows...")
    print("Checking for disconnected counter rows...")
    
    # Make a copy to avoid modifying original
    df = counters_df.copy()
    
    # Identify metadata columns (non-counter columns to exclude from the check)
    metadata_columns = [
        'FAB', 'ENTITY', 'FAB_ENTITY', 'DATE', 'Date', 'date', 'counter_date',
        'YEARWW', 'YearWW', 'yearww', 'FACILITY', 'Facility',
        'CEID', 'Ceid', 'TOOLSET', 'Toolset'
    ]
    
    # Get counter columns (all columns except metadata)
    metadata_upper = [m.upper() for m in metadata_columns]
    counter_columns = [
        col for col in df.columns 
        if col not in metadata_columns and col.upper() not in metadata_upper
    ]
    
    logger.info(f"Found {len(counter_columns)} counter columns to check")
    print(f"Found {len(counter_columns)} counter columns to check")
    
    # Create a subset with only counter columns for checking
    counter_data = df[counter_columns]
    
    logger.info("Converting counter columns to numeric...")
    print("Converting counter columns to numeric...")
    
    # Convert to numeric, coercing errors to NaN (handles string columns)
    counter_numeric = counter_data.apply(pd.to_numeric, errors='coerce')
    
    logger.info("Identifying valid vs disconnected values...")
    print("Identifying valid vs disconnected values...")
    
    # VECTORIZED: Check if each value is "invalid" (NULL or between -5 and 5)
    # A value is "valid" if it's not null AND outside the -5 to 5 range
    is_valid_value = counter_numeric.notna() & ((counter_numeric < -5) | (counter_numeric > 5))
    
    # Row is disconnected if NO columns have a valid value (all False in that row)
    # any(axis=1) returns True if ANY column is valid, so we negate it
    df['is_disconnected'] = ~is_valid_value.any(axis=1)
    
    disconnected_count = df['is_disconnected'].sum()
    logger.info(f"Found {disconnected_count} disconnected rows out of {len(df)} total")
    print(f"Found {disconnected_count} disconnected rows out of {len(df)} total")
    
    if disconnected_count == 0:
        logger.info("No disconnected rows to fix")
        print("No disconnected rows to fix")
        return df
    
    # Find date column
    date_col = None
    for col in ['counter_date', 'DATE', 'Date', 'date']:
        if col in df.columns:
            date_col = col
            break
    
    if date_col is None:
        logger.error("Could not find date column in counters data")
        print("ERROR: Could not find date column in counters data")
        raise ValueError("Date column not found in counters_df")
    
    # Find FAB_ENTITY column
    fab_entity_col = None
    for col in ['FAB_ENTITY', 'Fab_Entity', 'fab_entity']:
        if col in df.columns:
            fab_entity_col = col
            break
    
    if fab_entity_col is None:
        logger.error("Could not find FAB_ENTITY column in counters data")
        print("ERROR: Could not find FAB_ENTITY column in counters data")
        raise ValueError("FAB_ENTITY column not found in counters_df")
    
    logger.info(f"Using '{fab_entity_col}' for entity grouping and '{date_col}' for date sorting")
    print(f"Using '{fab_entity_col}' for entity grouping and '{date_col}' for date sorting")
    
    # Sort for proper forward-fill order
    logger.info("Sorting data by FAB_ENTITY and date...")
    print("Sorting data by FAB_ENTITY and date...")
    df = df.sort_values([fab_entity_col, date_col]).reset_index(drop=True)
    
    # VECTORIZED FORWARD-FILL:
    # For disconnected rows, replace counter values with last valid values per FAB_ENTITY
    logger.info("Forward-filling disconnected rows with last valid values...")
    print("Forward-filling disconnected rows with last valid values...")
    
    # Store original is_disconnected flags (before any modification)
    disconnected_flags = df['is_disconnected'].copy()
    
    # For each counter column, mask disconnected rows as NaN, then forward-fill within groups
    total_cols = len(counter_columns)
    for i, col in enumerate(counter_columns):
        if (i + 1) % 50 == 0 or (i + 1) == total_cols:
            logger.info(f"Processing column {i + 1}/{total_cols}")
            print(f"Processing column {i + 1}/{total_cols}")
        
        # Create a series where disconnected rows are NaN
        values_to_fill = df[col].copy()
        values_to_fill[disconnected_flags] = np.nan
        
        # Forward-fill within each FAB_ENTITY group
        df[col] = values_to_fill.groupby(df[fab_entity_col]).ffill()
    
    # Restore the is_disconnected flag (ffill doesn't affect it, but just to be safe)
    df['is_disconnected'] = disconnected_flags
    
    logger.info(f"Forward-fill complete. Fixed {disconnected_count} disconnected rows.")
    print(f"Forward-fill complete. Fixed {disconnected_count} disconnected rows.")
    
    return df
