# =========================================================================
# PRE-PROCESSING: Filter and fix counter data
# =========================================================================

# Step 1: Exclude non-chamber entities (LP, VTM, LLM have no counter data)
logger.info("PRE-PROCESSING: Filtering out non-chamber entities (LP, VTM, LLM)")
print("PRE-PROCESSING: Filtering out non-chamber entities (LP, VTM, LLM)")

before_filter = len(counters_df)
counters_df = counters_df[
    ~counters_df['ENTITY'].str.contains('_LP|_VTM|_LLM', case=False, regex=True)
].reset_index(drop=True)
after_filter = len(counters_df)

logger.info(f"Filtered out {before_filter - after_filter} non-chamber rows, {after_filter} rows remaining")
print(f"Filtered out {before_filter - after_filter} non-chamber rows, {after_filter} rows remaining")

# Step 2: Fix disconnected counter rows
logger.info("PRE-PROCESSING: Fixing disconnected counter rows")
print("PRE-PROCESSING: Fixing disconnected counter rows")

counters_df = fix_disconnected_counter_rows(counters_df)

print(f"PRE-PROCESSING COMPLETE - counters_df has {len(counters_df)} rows")
logger.info("Pre-processing complete")
print("Pre-processing complete")
# =========================================================================
