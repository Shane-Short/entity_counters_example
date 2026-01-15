# STEP 1: Find counter column using multi-pass approach
    # PASS 1: Look for counter > 1000 (normal operation)
    counter_found = self.find_counter_column(current_row)

    if not counter_found:
        # PASS 2: Look for counter > 100 (lower threshold)
        counter_found = self.find_counter_column_with_threshold(current_row, threshold=100)
        
        if counter_found:
            result['calculation_notes'].append('Used lower threshold (>100) to find counter')
    
    if not counter_found:
        # PASS 3: Look for first column with any numeric value (including 0.00)
        counter_found = self.find_first_numeric_counter(current_row)
        
        if counter_found:
            result['calculation_notes'].append('All counters at zero - using first available')
    
    if not counter_found:
        result['calculation_notes'].append('No valid counter column found')
        return result

    counter_col, current_val, _ = counter_found






def find_counter_column_with_threshold(
    self,
    row: pd.Series,
    threshold: float = 100
) -> Optional[Tuple[str, float, str]]:
    """
    Find first counter column with value above specified threshold.
    
    Parameters
    ----------
    row : pd.Series
        Row of counter data
    threshold : float
        Minimum value to consider valid
        
    Returns
    -------
    Tuple[str, float, str] or None
        (column_name, value, keyword) or None if not found
    """
    skip_columns = [
        'FAB', 'ENTITY', 'FAB_ENTITY', 'counter_date',
        'source_file', 'load_ww', 'load_ts', 'load_date',
        'file_modified_ts', 'counters_raw_id', 'is_disconnected'
    ]
    
    for col in row.index:
        if col in skip_columns:
            continue
        
        val = row[col]
        if pd.notna(val):
            try:
                num_val = float(val)
                if num_val > threshold:
                    keyword = col.split('_')[0] if '_' in col else col
                    return (col, num_val, keyword)
            except (ValueError, TypeError):
                continue
    
    return None


def find_first_numeric_counter(
    self,
    row: pd.Series
) -> Optional[Tuple[str, float, str]]:
    """
    Find first counter column with any numeric value (including 0.00).
    Used as fallback when all counters are zero.
    
    Parameters
    ----------
    row : pd.Series
        Row of counter data
        
    Returns
    -------
    Tuple[str, float, str] or None
        (column_name, value, keyword) or None if not found
    """
    skip_columns = [
        'FAB', 'ENTITY', 'FAB_ENTITY', 'counter_date',
        'source_file', 'load_ww', 'load_ts', 'load_date',
        'file_modified_ts', 'counters_raw_id', 'is_disconnected'
    ]
    
    for col in row.index:
        if col in skip_columns:
            continue
        
        val = row[col]
        if pd.notna(val):
            try:
                num_val = float(val)
                # Accept any numeric value, including 0.00
                keyword = col.split('_')[0] if '_' in col else col
                return (col, num_val, keyword)
            except (ValueError, TypeError):
                continue
    
    return None





# =========================================================
            # FIRST DAY BASELINE: If no previous row, establish baseline
            # =========================================================
            if previous_row is None:
                # Get is_disconnected flag if it exists
                is_disconnected = current_row.get("is_disconnected", False)
                
                # Use same multi-pass approach as calculate_wafer_production_single_row
                counter_found = self.find_counter_column(current_row)
                
                if not counter_found:
                    counter_found = self.find_counter_column_with_threshold(current_row, threshold=100)
                
                if not counter_found:
                    counter_found = self.find_first_numeric_counter(current_row)
                
                if counter_found:
                    counter_column, counter_value, counter_keyword = counter_found
                else:
                    counter_column = None
                    counter_value = None
                    counter_keyword = None
                
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







# Make a copy to avoid modifying original
df = counters_df.copy()

# =========================================================================
# DROP PROBLEMATIC COUNTER COLUMNS (known to provide inaccurate data)
# =========================================================================
columns_to_drop = [
    'PayForRFCounter',
    'DRYETCHLARRMIMMO9ADHOCCtr', 
    'NumWafersProcessed',
    'OOCCountSinceLastDown'
]

existing_cols_to_drop = [col for col in columns_to_drop if col in df.columns]
if existing_cols_to_drop:
    logger.info(f"Dropping problematic counter columns: {existing_cols_to_drop}")
    print(f"Dropping problematic counter columns: {existing_cols_to_drop}")
    df = df.drop(columns=existing_cols_to_drop)
# =========================================================================
