# Get running hours for this day
            date = current_row["counter_date"]
            
            # DEBUG: Check date types
            if entity_count == 1:  # Only log for first entity
                print(f"\nDEBUG running_hours lookup:")
                print(f"  Looking for FAB_ENTITY={fab_entity}, date={date} (type: {type(date)})")
                print(f"  state_hours has {len(state_hours_df)} rows")
                print(f"  state_hours FAB_ENTITY values: {state_hours_df['FAB_ENTITY'].unique()[:5]}")
                print(f"  state_hours date type: {type(state_hours_df['state_date'].iloc[0])}")
                print(f"  Sample state_hours dates: {state_hours_df['state_date'].unique()[:5]}")
            
            # Match FAB_ENTITY first
            fab_entity_match = state_hours_df[state_hours_df["FAB_ENTITY"] == fab_entity]
            
            if len(fab_entity_match) == 0:
                if entity_count == 1:
                    print(f"  WARNING: No state_hours rows for FAB_ENTITY={fab_entity}")
                running_hours = 0
            else:
                # Then match date - try both with and without conversion
                date_match = fab_entity_match[fab_entity_match["state_date"] == date]
                
                if len(date_match) == 0:
                    # Try converting date
                    date_as_date = pd.to_datetime(date).date() if not isinstance(date, type(pd.to_datetime('2020-01-01').date())) else date
                    date_match = fab_entity_match[fab_entity_match["state_date"] == date_as_date]
                
                if len(date_match) > 0:
                    running_hours = date_match["running_hours"].values[0]
                    if entity_count == 1:
                        print(f"  SUCCESS: Found running_hours={running_hours}")
                else:
                    running_hours = 0



# STEP 3: Track part replacements
    logger.info("STEP 3: Tracking part replacements")
    print("\nSTEP 3: Starting part replacement tracking...")
    
    # DEBUG: Check production_df
    part_repl_count = production_df['part_replacement_detected'].sum()
    print(f"  production_df has {len(production_df)} rows")
    print(f"  {part_repl_count} rows have part_replacement_detected=True")
    
    if part_repl_count > 0:
        sample_row = production_df[production_df['part_replacement_detected'] == True].iloc[0]
        print(f"  Sample all_part_replacements value: {sample_row['all_part_replacements']}")
        print(f"  Type: {type(sample_row['all_part_replacements'])}")
    
    replacements_df = track_part_replacements(config, production_df)
    
    print(f"  Part replacements returned: {len(replacements_df) if replacements_df is not None else 0} rows")
    
    if replacements_df is None or replacements_df.empty:
        logger.warning("No part replacements to load")
        print("  WARNING: No part replacements extracted!")
    else:
        print(f"  ✓ Successfully extracted {len(replacements_df)} part replacement events")
                    if entity_count == 1:
                        print(f"  WARNING: No date match. Available dates for this FAB_ENTITY: {fab_entity_match['state_date'].unique()[:5]}")
