def delete_existing_keys(
    self,
    df: pd.DataFrame,
    key_columns: list
) -> int:
    """
    Delete rows that would conflict with incoming data.
    
    Parameters
    ----------
    df : pd.DataFrame
        Incoming data
    key_columns : list
        Columns that form the unique key (e.g., ['FAB_ENTITY', 'state_date'])
        
    Returns
    -------
    int
        Number of rows deleted
    """
    if df.empty:
        return 0
    
    logger.info(f"Checking for existing keys to delete in {self.table_name}...")
    print(f"Checking for existing keys to delete in {self.table_name}...")
    
    conn = self.get_connection()
    cursor = conn.cursor()
    
    try:
        # Get unique key combinations from incoming data
        unique_keys = df[key_columns].drop_duplicates()
        total_deleted = 0
        
        # Build tuples for deletion
        key_tuples = list(unique_keys.itertuples(index=False, name=None))
        
        if len(key_tuples) == 0:
            return 0
        
        # Delete in batches to avoid parameter limits
        batch_size = 500
        total_batches = (len(key_tuples) + batch_size - 1) // batch_size
        
        for i in range(0, len(key_tuples), batch_size):
            batch = key_tuples[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            # Build WHERE clause for this batch
            if len(key_columns) == 1:
                col = key_columns[0]
                placeholders = ','.join(['?' for _ in batch])
                values = [t[0] for t in batch]
                
                delete_sql = f"""
                    DELETE FROM {self.schema}.{self.table_name}
                    WHERE [{col}] IN ({placeholders})
                """
                cursor.execute(delete_sql, values)
                
            else:
                # Multiple column composite key
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
            
            total_deleted += cursor.rowcount
            
            if total_batches > 1:
                print(f"  Delete batch {batch_num}/{total_batches}: {cursor.rowcount} rows")
        
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








def load_to_sqlserver_upsert(
    df: pd.DataFrame,
    config: Dict,
    table_params_key: str,
    key_columns: list,
    if_exists: str = "append",
) -> int:
    """
    Load DataFrame to SQL Server, deleting existing keys first (upsert pattern).
    
    Prevents duplicate key violations by removing existing rows before inserting.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to load
    config : dict
        Configuration dictionary
    table_params_key : str
        Key for table parameters in config
    key_columns : list
        Columns that form the unique key
    if_exists : str
        What to do if table exists
        
    Returns
    -------
    int
        Number of rows inserted
    """
    engine = SQLServerEngine(config, table_params_key)
    
    # Delete existing records that would conflict
    if key_columns and len(df) > 0:
        engine.delete_existing_keys(df, key_columns)
    
    # Insert new data
    return engine.load_dataframe(df, if_exists)
