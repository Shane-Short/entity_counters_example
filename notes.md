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
        
        Parameters
        ----------
        current_row : pd.Series
            Current day row
        previous_row : pd.Series
            Previous day row
        entity : str
            Entity name
        date : str
            Counter date
        
        Returns
        -------
        List[Dict]
            List of replacement dictionaries, one per counter that dropped
        """
        replacements = []
        
        # Get all counter columns
        counter_cols = [col for col in current_row.index if col.endswith('Counter')]
        
        for counter_col in counter_cols:
            current_val = current_row.get(counter_col)
            previous_val = previous_row.get(counter_col)
            
            # Skip if either value is missing
            if pd.isna(current_val) or pd.isna(previous_val):
                continue
            
            # Skip if values are too low (not actively used)
            if current_val < 100 or previous_val < 100:
                continue
            
            change = current_val - previous_val
            
            # Check for replacement (threshold from config)
            if change < self.replacement_threshold:
                replacements.append({
                    'counter_name': counter_col,
                    'previous_value': previous_val,
                    'current_value': current_val,
                    'change': change
                })
                
                logger.info(
                    f"PART REPLACEMENT - {entity} ({date}): {counter_col} "
                    f"dropped {change} (from {previous_val} to {current_val})"
                )
        
        return replacements








# STEP 5: Calculate wafers produced and wafers per hour
        if result["counter_change"] is not None and result["counter_change"] >= 0:
            result["wafers_produced"] = result["counter_change"]

            if running_hours > 0:
                result["wafers_per_hour"] = (
                    result["wafers_produced"] / running_hours
                )
            else:
                result["calculation_notes"].append(
                    "No running hours - cannot calculate wafers/hour"
                )

        # STEP 6: Check ALL counters for part replacements (not just the one used for wafer calc)
        if previous_row is not None:
            all_replacements = self.detect_all_part_replacements(
                current_row, previous_row, entity, date
            )
            
            if all_replacements:
                result['part_replacement_detected'] = True
                result['all_part_replacements'] = all_replacements  # Store list of all replacements
                result['calculation_notes'].append(
                    f"{len(all_replacements)} part replacement(s) detected across all counters"
                )
            else:
                result['part_replacement_detected'] = False
                result['all_part_replacements'] = []

        return result








def extract_replacements(
        self,
        production_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Extract ALL part replacement events from production data.
        Each counter that dropped gets its own row.
        
        Parameters
        ----------
        production_df : pd.DataFrame
            Wafer production data with part_replacement_detected and all_part_replacements columns
        
        Returns
        -------
        pd.DataFrame
            Part replacement events (one row per counter replaced)
        """
        logger.info("Extracting part replacement events")
        
        # Filter to rows where replacement was detected
        replacements = production_df[
            production_df["part_replacement_detected"] == True
        ].copy()
        
        if len(replacements) == 0:
            logger.info("No part replacements detected")
            return pd.DataFrame()
        
        # Expand all_part_replacements list into individual rows
        all_replacement_records = []
        
        for _, row in replacements.iterrows():
            # Get the list of all replacements for this entity-date
            replacement_list = row.get('all_part_replacements', [])
            
            if not replacement_list:
                # Fallback: if all_part_replacements doesn't exist, use the single counter
                replacement_list = [{
                    'counter_name': row['counter_column_used'],
                    'previous_value': row['counter_previous_value'],
                    'current_value': row['counter_current_value'],
                    'change': row['counter_change']
                }]
            
            # Create a separate record for each counter that was replaced
            for repl in replacement_list:
                all_replacement_records.append({
                    'FAB': row['FAB'],
                    'ENTITY': row['ENTITY'],
                    'FAB_ENTITY': row['FAB_ENTITY'],
                    'replacement_date': row['counter_date'],
                    'part_counter_name': repl['counter_name'],
                    'last_value_before_replacement': repl['previous_value'],
                    'first_value_after_replacement': repl['current_value'],
                    'value_drop': repl['previous_value'] - repl['current_value'],
                    'part_wafers_at_replacement': repl['previous_value'],
                    'notes': row.get('calculation_notes', ''),
                    'replacement_detected_ts': datetime.now()
                })
        
        replacement_events = pd.DataFrame(all_replacement_records)
        
        # Remove duplicates (same FAB_ENTITY + date + counter)
        before_dedup = len(replacement_events)
        replacement_events = replacement_events.drop_duplicates(
            subset=[
                "FAB_ENTITY",
                "replacement_date",
                "part_counter_name",
            ],
            keep="last",
        )
        after_dedup = len(replacement_events)
        
        if before_dedup > after_dedup:
            logger.info(
                f"Removed {before_dedup - after_dedup} "
                "duplicate replacement events"
            )
        
        logger.info(
            f"Part replacement tracking complete: "
            f"{len(replacement_events)} replacement events"
        )
        
        return replacement_events








