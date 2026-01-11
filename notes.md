def extract_replacements(self, production_df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract ALL part replacement events from production data.
        Creates one row per counter that was replaced.
        
        Parameters
        ----------
        production_df : pd.DataFrame
            Wafer production data with all_part_replacements column
        
        Returns
        -------
        pd.DataFrame
            Part replacement events (one row per replaced counter)
        """
        logger.info("Extracting part replacement events")
        
        # Filter to rows where replacement was detected
        replacements = production_df[
            production_df["part_replacement_detected"] == True
        ].copy()
        
        if len(replacements) == 0:
            logger.info("No part replacements detected")
            return pd.DataFrame()
        
        # Expand all_part_replacements into individual rows
        all_replacement_records = []
        
        for _, row in replacements.iterrows():
            # Get the list of all replacements for this entity-date
            replacement_list = row.get('all_part_replacements', [])
            
            # HANDLE CASE WHERE IT'S A STRING (from SQL storage)
            if isinstance(replacement_list, str):
                import ast
                try:
                    replacement_list = ast.literal_eval(replacement_list)
                except:
                    replacement_list = []
            
            # HANDLE EMPTY LIST
            if not replacement_list:
                logger.warning(f"part_replacement_detected=True but no replacements in list for {row['FAB_ENTITY']} on {row['counter_date']}")
                continue
            
            # CREATE A SEPARATE ROW FOR EACH REPLACED COUNTER
            for repl in replacement_list:
                all_replacement_records.append({
                    'FAB': row['FAB'],
                    'ENTITY': row['ENTITY'],
                    'FAB_ENTITY': row['FAB_ENTITY'],
                    'replacement_date': row['counter_date'],
                    'previous_counter_date': row.get('previous_counter_date'),  # ADD THIS
                    'part_counter_name': repl['counter_name'],
                    'last_value_before_replacement': repl['previous_value'],
                    'first_value_after_replacement': repl['current_value'],
                    'value_drop': abs(repl['change']),  # Absolute value of the drop
                    'part_wafers_at_replacement': repl['previous_value'],
                    'notes': f"Counter dropped {repl['change']} wafers",
                    'replacement_detected_ts': datetime.now()
                })
        
        if not all_replacement_records:
            logger.info("No valid part replacement records created")
            return pd.DataFrame()
        
        replacement_events = pd.DataFrame(all_replacement_records)
        
        # Remove duplicates
        before_dedup = len(replacement_events)
        replacement_events = replacement_events.drop_duplicates(
            subset=['FAB_ENTITY', 'replacement_date', 'part_counter_name'],
            keep='last'
        )
        after_dedup = len(replacement_events)
        
        if before_dedup > after_dedup:
            logger.info(f"Removed {before_dedup - after_dedup} duplicate replacement events")
        
        logger.info(f"Part replacement tracking complete: {len(replacement_events)} replacement events")
        
        return replacement_events
