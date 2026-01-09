def calculate_for_dataframe(
    self,
    counters_df: pd.DataFrame,
    state_hours_df: pd.DataFrame,
    mode: str = 'full'
) -> pd.DataFrame:
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
    logger.info(f"Starting wafer production calculation (mode: {mode})")

    # INCREMENTAL MODE: Filter to only new dates not already in database
    if mode == 'incremental':
        from utils.database_engine import get_database_connection
        
        try:
            logger.info("Incremental mode: Checking for existing production data")
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
            else:
                logger.info("Incremental mode: No existing data found, processing all records")
        except Exception as e:
            logger.warning(f"Could not check existing production data: {e}. Processing all records.")

    # Filter out excluded entities (e.g., loadports)
    if self.exclude_patterns:
        initial_count = len(counters_df)
        excluded_entities = counters_df["ENTITY"].apply(self.is_entity_excluded)
        excluded_count = excluded_entities.sum()

        if excluded_count > 0:
            excluded_list = (
                counters_df[excluded_entities]["ENTITY"]
                .unique()
                .tolist()
            )
            logger.info(
                f"Excluding {excluded_count} rows from "
                f"{len(excluded_list)} entities matching exclude patterns"
            )
            logger.info(f"Sample excluded entities: {excluded_list[:5]}")
            counters_df = (
                counters_df[~excluded_entities]
                .reset_index(drop=True)
            )
            logger.info(
                f"Remaining entities for wafer production: "
                f"{len(counters_df)}"
            )

    # Sort by entity and date
    counters_df = (
        counters_df
        .sort_values(["ENTITY", "counter_date"])
        .reset_index(drop=True)
    )

    results = []

    # Group by FAB_ENTITY
    for fab_entity, entity_group in counters_df.groupby("FAB_ENTITY"):
        entity = entity_group.iloc[0]["ENTITY"]
        entity_group = (
            entity_group
            .sort_values("counter_date")
            .reset_index(drop=True)
        )

        # Skip if only 1 day of data - can't calculate production without previous day
        if len(entity_group) == 1:
            logger.debug(
                f"Skipping {entity}: only 1 day of data, "
                "no previous comparison possible"
            )

            # Still create a result row but with no wafer calculation
            result = {
                "FAB": entity_group.iloc[0].get("FAB", ""),
                "ENTITY": entity,
                "FAB_ENTITY": entity_group.iloc[0].get("FAB_ENTITY", ""),
                "counter_date": entity_group.iloc[0]["counter_date"],
                "counter_column_used": None,
                "counter_keyword_used": None,
                "counter_current_value": None,
                "counter_previous_value": None,
                "counter_change": None,
                "part_replacement_detected": False,
                "wafers_produced": None,
                "running_hours": 0,
                "wafers_per_hour": None,
                "calculation_notes": [
                    "Only 1 day of data - no previous comparison"
                ],
            }
            results.append(result)
            continue

        # Process each day
        for idx, current_row in entity_group.iterrows():
            # Get previous row (only within same entity)
            previous_row = (
                entity_group.iloc[idx - 1] if idx > 0 else None
            )

            if previous_row is None:
                logger.debug(
                    f"{entity} on {current_row['counter_date']}: "
                    "First day, no previous row"
                )

            # Get running hours for this day
            date = current_row["counter_date"]
            running_hours_row = state_hours_df[
                (state_hours_df["ENTITY"] == entity)
                & (state_hours_df["state_date"] == date)
            ]

            running_hours = (
                running_hours_row["running_hours"].values[0]
                if len(running_hours_row) > 0
                else 0
            )

            # Calculate production
            result = self.calculate_wafer_production_single_row(
                current_row,
                previous_row,
                running_hours,
            )
            results.append(result)

    # Convert to DataFrame
    production_df = pd.DataFrame(results)

    # Convert notes list to string
    production_df["calculation_notes"] = (
        production_df["calculation_notes"]
        .apply(lambda x: " | ".join(x) if x else None)
    )

    # Remove duplicates based on FAB_ENTITY and counter_date
    before_dedup = len(production_df)
    production_df = production_df.drop_duplicates(
        subset=["FAB_ENTITY", "counter_date"],
        keep="last",
    )
    after_dedup = len(production_df)

    if before_dedup > after_dedup:
        logger.info(
            f"Removed {before_dedup - after_dedup} duplicate rows"
        )

    logger.info(
        f"Wafer production calculation complete: "
        f"{len(production_df)} rows"
    )
    logger.info(
        f"Rows with wafers calculated: "
        f"{production_df['wafers_produced'].notna().sum()}"
    )
    logger.info(
        f"Part replacements detected: "
        f"{production_df['part_replacement_detected'].sum()}"
    )

    return production_df





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
