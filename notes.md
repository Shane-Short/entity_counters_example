def fix_disconnected_counter_rows(counters_df: pd.DataFrame) -> pd.DataFrame:
    """
    Detects disconnected rows in counter data and forward/back-fills with valid values.
    
    A row is considered "disconnected" if ALL counter value columns are either:
    - NULL/NaN, OR
    - Between -5 and 5 (essentially zero or near-zero noise)
    
    For disconnected rows:
    - Counter values are forward-filled from the last valid row for that FAB_ENTITY
    - If no previous valid row exists, values are back-filled from the next valid row
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
        'CEID', 'Ceid', 'TOOLSET', 'Toolset',
        'counters_raw_id', 'source_file', 'load_ww', 'load_ts', 
        'load_date', 'file_modified_ts'
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
    
    # Store original is_disconnected flags (before any modification)
    disconnected_flags = df['is_disconnected'].copy()
    
    # =========================================================================
    # FORWARD-FILL: Fill disconnected rows with last valid values
    # =========================================================================
    logger.info("Forward-filling disconnected rows with last valid values...")
    print("Forward-filling disconnected rows with last valid values...")
    
    total_cols = len(counter_columns)
    for i, col in enumerate(counter_columns):
        if (i + 1) % 50 == 0 or (i + 1) == total_cols:
            logger.info(f"Forward-filling column {i + 1}/{total_cols}")
            print(f"Forward-filling column {i + 1}/{total_cols}")
        
        # Create a series where disconnected rows are NaN
        values_to_fill = df[col].copy()
        values_to_fill[disconnected_flags] = np.nan
        
        # Forward-fill within each FAB_ENTITY group
        df[col] = values_to_fill.groupby(df[fab_entity_col]).ffill()
    
    logger.info("Forward-fill complete.")
    print("Forward-fill complete.")
    
    # =========================================================================
    # BACKFILL: Handle cases where disconnected rows are at the START of data
    # (no previous valid row to forward-fill from)
    # =========================================================================
    logger.info("Backfilling any remaining disconnected rows (start-of-data gaps)...")
    print("Backfilling any remaining disconnected rows (start-of-data gaps)...")
    
    backfill_needed = False
    for i, col in enumerate(counter_columns):
        # Check if any disconnected rows still have NaN after forward-fill
        still_nan = df[col].isna() & disconnected_flags
        if still_nan.any():
            if not backfill_needed:
                backfill_needed = True
                logger.info("Found rows needing backfill, processing...")
                print("Found rows needing backfill, processing...")
            
            # Backfill within each FAB_ENTITY group
            df[col] = df[col].groupby(df[fab_entity_col]).bfill()
    
    if backfill_needed:
        # Count how many were backfilled
        backfilled_entities = df[disconnected_flags].groupby(fab_entity_col).size()
        logger.info(f"Backfill complete. Affected {len(backfilled_entities)} entities.")
        print(f"Backfill complete. Affected {len(backfilled_entities)} entities.")
    else:
        logger.info("No backfill needed - all disconnected rows were forward-filled.")
        print("No backfill needed - all disconnected rows were forward-filled.")
    
    # Restore the is_disconnected flag
    df['is_disconnected'] = disconnected_flags
    
    logger.info(f"Disconnected row fix complete. Fixed {disconnected_count} rows.")
    print(f"Disconnected row fix complete. Fixed {disconnected_count} rows.")
    
    return df
