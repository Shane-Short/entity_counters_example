# DEBUG: Check for NULLs in DataFrame before conversion
        logger.info(f"Checking DataFrame for NULL values before insert into {self.table_name}")
        for col in df.columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                logger.error(f"  Column '{col}' has {null_count} NULL values")
        
        # Convert DataFrame to list of tuples, replacing NaN with None
        df_clean = df.replace({pd.NA: None, float('nan'): None, float('inf'): None, float('-inf'): None})
        data_tuples = [tuple(None if pd.isna(x) else x for x in row) for row in df_clean.values]
