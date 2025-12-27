def load_dataframe(self, df: pd.DataFrame, if_exists: str = 'append') -> int:
        """
        Load DataFrame to SQL Server table.
        """
        if df.empty:
            logger.warning("DataFrame is empty, nothing to load")
            return 0
        
        logger.info(f"Loading {len(df)} rows to {self.table_name}")
        
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
            
            # Convert DataFrame to list of tuples
            data_tuples = [tuple(row) for row in df.values]
            
            logger.info(f"Inserting {len(data_tuples)} rows using fast_executemany")
            
            # Use fast_executemany for bulk insert
            cursor.fast_executemany = True
            cursor.executemany(insert_sql, data_tuples)
            conn.commit()
            
            rows_inserted = len(data_tuples)
            logger.info(f"Successfully loaded {rows_inserted} rows to {self.table_name}")
            
            return rows_inserted
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error loading data to {self.table_name}: {e}")
            raise
        
        finally:
            cursor.close()
            conn.close()
