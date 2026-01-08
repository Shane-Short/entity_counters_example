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





def detect_all_part_replacements(
        self,
        current_row: pd.Series,
        previous_row: pd.Series,
        entity: str,
        date: str
    ) -> List[Dict]:
        """
        Check ALL counter columns for part replacements.
        
        Returns list of replacement events (one per counter that dropped).
        """
        replacements = []
        
        # Get all counter columns
        counter_cols = [col for col in current_row.index if col.endswith('Counter')]
        
        for counter_col in counter_cols:
            current_val = current_row.get(counter_col)
            previous_val = previous_row.get(counter_col) if previous_row is not None else None
            
            # Skip if either value is missing
            if pd.isna(current_val) or pd.isna(previous_val):
                continue
            
            # Skip if values are too low (not actively used)
            if current_val < 100 or previous_val < 100:
                continue
            
            change = current_val - previous_val
            
            # Check for replacement (threshold: -10)
            if change < self.replacement_threshold:
                replacements.append({
                    'counter_name': counter_col,
                    'previous_value': previous_val,
                    'current_value': current_val,
                    'change': change
                })
                
                logger.info(f"PART REPLACEMENT - {entity} ({date}): {counter_col} dropped {change} (from {previous_val} to {current_val})")
        
        return replacements







# After calculating wafer production...
        
        # Check for part replacements in ALL counters
        if previous_row is not None:
            all_replacements = self.detect_all_part_replacements(current_row, previous_row, entity, date)
            
            if all_replacements:
                result['part_replacement_detected'] = True
                result['part_replacements_detail'] = all_replacements  # Store all detected replacements
                result['calculation_notes'].append(f"{len(all_replacements)} part replacement(s) detected")
            else:
                result['part_replacement_detected'] = False
                result['part_replacements_detail'] = []
        
        # Rest of existing code...
