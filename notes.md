def calculate_for_dataframe(self, counters_df: pd.DataFrame, state_hours_df: pd.DataFrame, mode: str = 'full') -> pd.DataFrame:
        """
        Calculate wafer production for entire DataFrame.
        
        Parameters
        ----------
        counters_df : pd.DataFrame
            Counters data (from Bronze)
        state_hours_df : pd.DataFrame
            State hours data (running hours by entity-date)
        mode : str
            'full' = process all dates, 'incremental' = only process new dates
        
        Returns
        -------
        pd.DataFrame
            Daily production metrics
        """
        logger.info("Starting wafer production calculation")
        
        # INCREMENTAL MODE: Filter to only new dates
        if mode == 'incremental':
            # Get existing production dates from database
            from utils.database_engine import get_database_connection
            
            try:
                conn = get_database_connection(self.config)
                existing_dates_query = """
                    SELECT DISTINCT FAB_ENTITY, counter_date 
                    FROM dbo.wafer_production
                """
                existing_df = pd.read_sql(existing_dates_query, conn)
                conn.close()
                
                if not existing_df.empty:
                    # Create a set of (FAB_ENTITY, counter_date) tuples that already exist
                    existing_keys = set(zip(existing_df['FAB_ENTITY'], existing_df['counter_date']))
                    
                    # Filter counters_df to only new records
                    before_filter = len(counters_df)
                    counters_df = counters_df[
                        ~counters_df.apply(lambda row: (row['FAB_ENTITY'], row['counter_date']) in existing_keys, axis=1)
                    ].reset_index(drop=True)
                    after_filter = len(counters_df)
                    
                    logger.info(f"Incremental mode: Filtered out {before_filter - after_filter} existing records, processing {after_filter} new records")
            except Exception as e:
                logger.warning(f"Could not check existing production data: {e}. Processing all records.")
        
        # Rest of existing code...
