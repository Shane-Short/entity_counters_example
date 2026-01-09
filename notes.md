def bulk_insert_fast(self, df: pd.DataFrame) -> int:
    """
    Ultra-fast bulk insert using SQL Server's native bulk copy.
    10-100x faster than executemany.
    """
    import tempfile
    import csv
    
    if df.empty:
        return 0
    
    logger.info(f"Fast bulk insert: {len(df)} rows to {self.table_name}")
    
    # Write to temporary CSV
    with tempfile.NamedTemporaryFile(mode='w', delete=False, newline='', suffix='.csv') as tmp:
        df.to_csv(tmp, index=False, header=False, quoting=csv.QUOTE_MINIMAL)
        tmp_path = tmp.name
    
    try:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Build column list
        columns_str = ','.join([f"[{col}]" for col in df.columns])
        
        # Use BULK INSERT (fastest method)
        bulk_sql = f"""
        BULK INSERT {self.schema}.{self.table_name}
        FROM '{tmp_path}'
        WITH (
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\\n',
            FIRSTROW = 1,
            TABLOCK
        )
        """
        
        print(f"Starting fast bulk insert of {len(df)} rows...")
        start = time.time()
        cursor.execute(bulk_sql)
        conn.commit()
        elapsed = time.time() - start
        
        print(f"✓ Bulk insert complete in {elapsed:.1f} seconds ({len(df)/elapsed:.0f} rows/sec)")
        
        cursor.close()
        conn.close()
        
        return len(df)
        
    finally:
        import os
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
