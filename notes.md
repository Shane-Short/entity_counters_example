def delete_existing_keys(
    self,
    df: pd.DataFrame,
    key_columns: list
) -> int:
    """
    Delete rows that would conflict with incoming data using date range.
    Much faster than row-by-row deletion.
    """
    if df.empty:
        return 0
    
    logger.info(f"Checking for existing keys to delete in {self.table_name}...")
    print(f"Checking for existing keys to delete in {self.table_name}...")
    
    conn = self.get_connection()
    cursor = conn.cursor()
    
    try:
        # Find date column in key_columns
        date_col = None
        for col in key_columns:
            if 'date' in col.lower():
                date_col = col
                break
        
        if date_col and date_col in df.columns:
            # Fast path: delete by date range
            min_date = df[date_col].min()
            max_date = df[date_col].max()
            
            delete_sql = f"""
                DELETE FROM {self.schema}.{self.table_name}
                WHERE [{date_col}] >= ? AND [{date_col}] <= ?
            """
            
            print(f"Deleting existing rows for date range: {min_date} to {max_date}")
            cursor.execute(delete_sql, (min_date, max_date))
            
        else:
            # Fallback: delete by unique key combinations (slower)
            unique_keys = df[key_columns].drop_duplicates()
            key_tuples = list(unique_keys.itertuples(index=False, name=None))
            
            batch_size = 500
            for i in range(0, len(key_tuples), batch_size):
                batch = key_tuples[i:i + batch_size]
                
                conditions = ' OR '.join([
                    '(' + ' AND '.join([f"[{col}] = ?" for col in key_columns]) + ')'
                    for _ in batch
                ])
                
                params = []
                for t in batch:
                    params.extend(t)
                
                delete_sql = f"""
                    DELETE FROM {self.schema}.{self.table_name}
                    WHERE {conditions}
                """
                cursor.execute(delete_sql, params)
        
        total_deleted = cursor.rowcount
        conn.commit()
        
        if total_deleted > 0:
            logger.info(f"Deleted {total_deleted} existing rows from {self.table_name}")
            print(f"Deleted {total_deleted} existing rows from {self.table_name}")
        else:
            logger.info(f"No existing rows to delete in {self.table_name}")
            print(f"No existing rows to delete in {self.table_name}")
        
        return total_deleted
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error deleting existing keys from {self.table_name}: {e}")
        raise
        
    finally:
        cursor.close()
        conn.close()













def add_location_column(df: pd.DataFrame, fab_column: str = 'FAB') -> pd.DataFrame:
    """
    Adds Location column based on FAB mapping.
    """
    FAB_TO_LOCATION = {
        "D1D": "Portland",
        "D1X": "Portland",
        "D1C": "Portland",
        "AFO": "Portland",
        "F11X": "Albuquerque",
        "F12C": "Arizona",
        "F21": "Albuquerque",
        "F24": "Ireland",
        "F28": "Israel",
        "F32": "Arizona",
        "F42": "Arizona",
        "F52": "Arizona",
    }
    
    def map_fab(fab_val):
        if isinstance(fab_val, str):
            for fab_code, loc in FAB_TO_LOCATION.items():
                if fab_code in fab_val:
                    return loc
        return "Unknown"
    
    df['Location'] = df[fab_column].apply(map_fab)
    return df











