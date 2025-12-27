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
            
            # Enhanced error logging - check which column is causing the issue
            error_msg = str(e)
            logger.error(f"Error loading data to {self.table_name}: {e}")
            
            if 'String data, right truncation' in error_msg:
                logger.error("STRING TRUNCATION ERROR DETECTED - Checking column lengths...")
                
                # Check max length of each string column
                for col in df.columns:
                    if df[col].dtype == 'object':
                        max_len = df[col].astype(str).str.len().max()
                        logger.error(f"  Column: {col:30} Max Length: {max_len}")
                        
                        # Show sample of longest values
                        longest_idx = df[col].astype(str).str.len().idxmax()
                        longest_val = str(df[col].iloc[longest_idx])
                        logger.error(f"    Longest value ({len(longest_val)} chars): {longest_val[:100]}...")
            
            raise
