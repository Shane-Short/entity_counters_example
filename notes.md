def bulk_insert_batched(self, df: pd.DataFrame, if_exists: str = 'append') -> int:
        """
        Fast bulk insert using batching with optimizations.
        Much faster than single executemany.
        """
        if df.empty:
            logger.warning("DataFrame is empty, nothing to load")
            return 0
        
        logger.info(f"Loading {len(df)} rows to {self.table_name} using fast batched insert")
        
        # Get FRESH connection
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Get column names
            columns = df.columns.tolist()
            
            # Build INSERT statement
            placeholders = ','.join(['?' for _ in columns])
            columns_str = ','.join([f"[{col}]" for col in columns])
            
            insert_sql = f"""
                INSERT INTO {self.schema}.{self.table_name} ({columns_str})
                VALUES ({placeholders})
            """
            
            # Convert DataFrame - handle lists/arrays properly
            df_clean = df.copy()
            for col in df_clean.columns:
                # Convert lists to strings
                if df_clean[col].apply(lambda x: isinstance(x, (list, dict))).any():
                    df_clean[col] = df_clean[col].apply(lambda x: str(x) if isinstance(x, (list, dict)) else x)
            
            # Replace NaN/None
            df_clean = df_clean.replace({pd.NA: None, float('nan'): None, float('inf'): None, float('-inf'): None})
            data_tuples = [tuple(None if pd.isna(x) else x for x in row) for row in df_clean.values]
            
            # BATCH PROCESSING with commit after each batch
            batch_size = 50000  # Larger batches
            total_rows = len(data_tuples)
            
            if total_rows > batch_size:
                logger.info(f"Using batch processing with batch_size={batch_size}")
                
                rows_inserted = 0
                for i in range(0, total_rows, batch_size):
                    batch = data_tuples[i:i + batch_size]
                    batch_num = (i // batch_size) + 1
                    total_batches = (total_rows + batch_size - 1) // batch_size
                    
                    print(f"  Inserting batch {batch_num}/{total_batches} ({len(batch)} rows)...")
                    
                    import time
                    start = time.time()
                    cursor.fast_executemany = True
                    cursor.executemany(insert_sql, batch)
                    conn.commit()
                    elapsed = time.time() - start
                    
                    rows_inserted += len(batch)
                    rate = len(batch) / elapsed if elapsed > 0 else 0
                    print(f"  ✓ Batch {batch_num}/{total_batches} complete in {elapsed:.1f}s ({rate:.0f} rows/sec)")
                
                logger.info(f"Successfully loaded {rows_inserted} rows in {total_batches} batches")
                return rows_inserted
            
            else:
                # Small dataset
                import time
                print(f"Starting SQL Server insert of {len(data_tuples)} rows to {self.table_name}...")
                start_time = time.time()
                cursor.fast_executemany = True
                cursor.executemany(insert_sql, data_tuples)
                elapsed = time.time() - start_time
                
                print(f"Insert complete in {elapsed:.1f} seconds, committing...")
                conn.commit()
                
                rows_inserted = len(data_tuples)
                print(f"✓ Successfully loaded {rows_inserted} rows")
                return rows_inserted
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error loading data to {self.table_name}: {e}")
            raise
        
        finally:
            cursor.close()
            conn.close()






