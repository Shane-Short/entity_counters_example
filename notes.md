replacement_events = pd.DataFrame(all_replacement_records)
        
        # DEBUG: Check for duplicates BEFORE deduplication
        duplicate_check = replacement_events.groupby(['FAB_ENTITY', 'replacement_date', 'part_counter_name']).size()
        duplicates_found = duplicate_check[duplicate_check > 1]
        
        if len(duplicates_found) > 0:
            print(f"\nWARNING: Found {len(duplicates_found)} duplicate combinations BEFORE deduplication:")
            print(duplicates_found.head(10))
            
            # Show details of first duplicate
            first_dup = duplicates_found.index[0]
            dup_rows = replacement_events[
                (replacement_events['FAB_ENTITY'] == first_dup[0]) &
                (replacement_events['replacement_date'] == first_dup[1]) &
                (replacement_events['part_counter_name'] == first_dup[2])
            ]
            print(f"\nDetails of duplicate rows for {first_dup}:")
            print(dup_rows[['FAB_ENTITY', 'replacement_date', 'part_counter_name', 'last_value_before_replacement', 'first_value_after_replacement']])
        
        # Remove duplicates
        before_dedup = len(replacement_events)
