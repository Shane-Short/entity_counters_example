# Handle case-insensitive duplicates by renaming
        seen_lower = {}  # Track lowercase versions with their count
        renamed_columns = []
        
        for col in part_counter_columns:
            col_lower = col.lower()
            
            if col_lower not in seen_lower:
                # First occurrence - keep as-is
                seen_lower[col_lower] = 1
                renamed_columns.append(col)
            else:
                # Case-insensitive duplicate - add suffix
                seen_lower[col_lower] += 1
                suffix_num = seen_lower[col_lower]
                new_name = f"{col}_{suffix_num}"
                renamed_columns.append(new_name)
                logger.warning(f"Case-insensitive duplicate detected: '{col}' renamed to '{new_name}'")
        
        part_counter_columns = renamed_columns
        logger.info(f"After handling duplicates: {len(part_counter_columns)} counter columns (with renames)")




# Handle duplicate column names (case-insensitive)
# Match the renaming logic from setup_counters_table.py
cols_lower = {}
new_columns = []
for col in df.columns:
    col_lower = col.lower()
    if col_lower not in cols_lower:
        cols_lower[col_lower] = 1
        new_columns.append(col)
    else:
        cols_lower[col_lower] += 1
        new_col = f"{col}_{cols_lower[col_lower]}"
        new_columns.append(new_col)
        self.logger.warning(f"Duplicate column '{col}' renamed to '{new_col}'")

df.columns = new_columns
