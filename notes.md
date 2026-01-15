def fix_disconnected_counter_rows(counters_df: pd.DataFrame) -> pd.DataFrame:
    """
    Detects and fixes disconnected/zero counter values at the CELL level.
    
    For each counter column individually:
    - If value is NULL or between -5 and 5, forward-fill from previous day
    - If no previous value exists, backfill from next valid value
    
    Also flags rows where ALL counter values were invalid (fully disconnected).
    
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
    
    logger.info("Starting cell-level counter fix...")
    print("Starting cell-level counter fix...")
    
    # Make a copy to avoid modifying original
    df = counters_df.copy()
    
    # Identify metadata columns (non-counter columns to exclude)
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
    
    logger.info(f"Found {len(counter_columns)} counter columns to process")
    print(f"Found {len(counter_columns)} counter columns to process")
    
    # Find date column
    date_col = None
    for col in ['counter_date', 'DATE', 'Date', 'date']:
        if col in df.columns:
            date_col = col
            break
    
    if date_col is None:
        raise ValueError("Date column not found in counters_df")
    
    # Find FAB_ENTITY column
    fab_entity_col = None
    for col in ['FAB_ENTITY', 'Fab_Entity', 'fab_entity']:
        if col in df.columns:
            fab_entity_col = col
            break
    
    if fab_entity_col is None:
        raise ValueError("FAB_ENTITY column not found in counters_df")
    
    logger.info(f"Using '{fab_entity_col}' for entity grouping and '{date_col}' for date sorting")
    print(f"Using '{fab_entity_col}' for entity grouping and '{date_col}' for date sorting")
    
    # Sort for proper forward-fill order
    logger.info("Sorting data by FAB_ENTITY and date...")
    print("Sorting data by FAB_ENTITY and date...")
    df = df.sort_values([fab_entity_col, date_col]).reset_index(drop=True)
    
    # Track which cells were fixed for reporting
    total_cells_fixed = 0
    
    # =========================================================================
    # CELL-LEVEL FORWARD-FILL AND BACKFILL
    # =========================================================================
    logger.info("Processing counter columns (cell-level forward/back-fill)...")
    print("Processing counter columns (cell-level forward/back-fill)...")
    
    total_cols = len(counter_columns)
    for i, col in enumerate(counter_columns):
        if (i + 1) % 50 == 0 or (i + 1) == total_cols:
            logger.info(f"Processing column {i + 1}/{total_cols}: {col}")
            print(f"Processing column {i + 1}/{total_cols}: {col}")
        
        # Convert to numeric
        numeric_values = pd.to_numeric(df[col], errors='coerce')
        
        # Identify cells that need fixing: NULL or between -5 and 5
        needs_fix = numeric_values.isna() | ((numeric_values >= -5) & (numeric_values <= 5))
        
        cells_to_fix = needs_fix.sum()
        if cells_to_fix > 0:
            total_cells_fixed += cells_to_fix
            
            # Replace bad values with NaN for filling
            fixed_values = numeric_values.copy()
            fixed_values[needs_fix] = np.nan
            
            # Forward-fill within each FAB_ENTITY group
            fixed_values = fixed_values.groupby(df[fab_entity_col]).ffill()
            
            # Backfill any remaining NaN (for first days with no previous value)
            fixed_values = fixed_values.groupby(df[fab_entity_col]).bfill()
            
            # Update the column
            df[col] = fixed_values
    
    logger.info(f"Cell-level fix complete. Fixed {total_cells_fixed} cells across all columns.")
    print(f"Cell-level fix complete. Fixed {total_cells_fixed} cells across all columns.")
    
    # =========================================================================
    # FLAG FULLY DISCONNECTED ROWS (all counters were invalid)
    # =========================================================================
    logger.info("Identifying fully disconnected rows...")
    print("Identifying fully disconnected rows...")
    
    # Re-check original data to see which rows had ALL counters invalid
    counter_data = counters_df[counter_columns]
    counter_numeric = counter_data.apply(pd.to_numeric, errors='coerce')
    
    # A value is "valid" if it's not null AND outside the -5 to 5 range
    is_valid_value = counter_numeric.notna() & ((counter_numeric < -5) | (counter_numeric > 5))
    
    # Row is fully disconnected if NO columns had a valid value
    df['is_disconnected'] = ~is_valid_value.any(axis=1)
    
    disconnected_count = df['is_disconnected'].sum()
    logger.info(f"Fully disconnected rows: {disconnected_count} out of {len(df)} total")
    print(f"Fully disconnected rows: {disconnected_count} out of {len(df)} total")
    
    logger.info("Counter fix complete.")
    print("Counter fix complete.")
    
    return df







if previous_row is None:
                logger.debug(
                    f"{entity} on {current_row['counter_date']}: "
                    f"First day, no previous row"
                )

            # Get running hours for this day
            date = current_row["counter_date"]

            # Match FAB_ENTITY first
            fab_entity_match = state_hours_df[
                state_hours_df["FAB_ENTITY"] == fab_entity
            ]

            if len(fab_entity_match) == 0:
                if entity_count == 1:
                    print(
                        f" WARNING: No state_hours rows for FAB_ENTITY={fab_entity}"
                    )
                running_hours = 0
                is_bagged = False
            else:
                # Then match date - try both with and without conversion
                date_match = fab_entity_match[
                    fab_entity_match["state_date"] == date
                ]

                if len(date_match) == 0:
                    # Try converting date
                    date_as_date = (
                        pd.to_datetime(date).date()
                        if not isinstance(
                            date,
                            type(pd.to_datetime("2020-01-01").date()),
                        )
                        else date
                    )

                    date_match = fab_entity_match[
                        fab_entity_match["state_date"] == date_as_date
                    ]

                if len(date_match) > 0:
                    running_hours = date_match["running_hours"].values[0]
                    is_bagged = (
                        bool(date_match["is_bagged"].values[0])
                        if "is_bagged" in date_match.columns
                        else False
                    )
                    if entity_count == 1:
                        print(
                            f" SUCCESS: Found running_hours={running_hours}, "
                            f"is_bagged={is_bagged}"
                        )
                else:
                    running_hours = 0
                    is_bagged = False

            # =========================================================
            # FIRST DAY BASELINE: If no previous row, establish baseline
            # =========================================================
            if previous_row is None:
                # Find the counter column and value for this entity
                # (We need to determine what counter is being used)
                counter_value = None
                counter_column = None
                counter_keyword = None
                
                # Get is_disconnected flag if it exists
                is_disconnected = current_row.get("is_disconnected", False)
                
                # Search for a valid counter value in this row
                for col in current_row.index:
                    if col in ['FAB', 'ENTITY', 'FAB_ENTITY', 'counter_date', 
                               'source_file', 'load_ww', 'load_ts', 'load_date',
                               'file_modified_ts', 'counters_raw_id', 'is_disconnected']:
                        continue
                    val = current_row[col]
                    if pd.notna(val):
                        try:
                            num_val = float(val)
                            if num_val > 5:  # Valid counter value
                                counter_value = num_val
                                counter_column = col
                                # Extract keyword if column has underscore
                                if '_' in col:
                                    counter_keyword = col.split('_')[0]
                                else:
                                    counter_keyword = col
                                break
                        except (ValueError, TypeError):
                            continue
                
                result = {
                    "FAB": current_row.get("FAB", ""),
                    "ENTITY": entity,
                    "FAB_ENTITY": current_row.get("FAB_ENTITY", ""),
                    "counter_date": current_row["counter_date"],
                    "previous_counter_date": None,
                    "counter_column_used": counter_column,
                    "counter_keyword_used": counter_keyword,
                    "counter_current_value": counter_value,
                    "counter_previous_value": counter_value,  # Same as current = baseline
                    "counter_change": 0,
                    "part_replacement_detected": False,
                    "all_part_replacements": [],
                    "wafers_produced": 0,  # Baseline = 0 wafers
                    "running_hours": running_hours,
                    "wafers_per_hour": 0,
                    "is_disconnected": is_disconnected,
                    "calculation_notes": [
                        "First day - baseline established"
                    ],
                }
                
                results.append(result)
                continue  # Skip to next row
            # =========================================================

            # Calculate production (for non-first-day rows)
            result = self.calculate_wafer_production_single_row(
                current_row,
                previous_row,
                running_hours,
                is_bagged,
            )
            
            # Carry through is_disconnected flag from counters data
            result["is_disconnected"] = current_row.get("is_disconnected", False)

            results.append(result)






