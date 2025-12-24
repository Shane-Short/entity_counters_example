# Add each part counter column
        for i, col in enumerate(part_counter_columns):
            # Clean column name (remove brackets, quotes, special chars)
            clean_col = col.replace('[', '').replace(']', '').replace("'", '').replace('"', '')
            
            # SQL Server column name limit is 128 characters
            if len(clean_col) > 128:
                clean_col = clean_col[:128]
                logger.warning(f"Column name truncated to 128 chars: {col}")
            
            sql_lines.append(f"    [{clean_col}] DECIMAL(18,2),")
