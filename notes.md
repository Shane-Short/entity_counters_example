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
                    if entity_count == 1:
                        print(f"  WARNING: No date match. Available dates for this FAB_ENTITY: {fab_entity_match['state_date'].unique()[:5]}")
