# ---------------------------------------------------------------------------
# DEBUG HELPER — remove or comment out once root cause is confirmed
# ---------------------------------------------------------------------------
def debug_age_and_active(df_mom):
    """
    Prints diagnostic info to identify why age buckets or active counts look wrong.
    Check this output before drawing any conclusions about the data.
    """
    print("
" + "=" * 60)
    print("DEBUG: Age bucket + active status diagnostics")
    print("=" * 60)

    # 1) Show today's date being used as the reference point
    print(f"
  ASOF_DATE (today): {ASOF_DATE.date()}")

    # 2) Show raw Warranty_Start samples BEFORE any parsing
    #    — reveals if values are strings, ints (Excel serials), nulls, etc.
    print(f"
  Warranty_Start dtype from SQL : {df_mom['Warranty_Start'].dtype}")
    print(f"  Sample raw values (first 10) :")
    print(df_mom["Warranty_Start"].head(10).to_string())

    # 3) Parse dates and compute ages — same logic as add_age_bucket()
    ws = pd.to_datetime(df_mom["Warranty_Start"], errors="coerce")
    age_years = (ASOF_DATE - ws).dt.days / 365.25

    print(f"
  Warranty_Start null count   : {ws.isna().sum():,} of {len(ws):,}")
    print(f"  Parsed date range           : {ws.min()} → {ws.max()}")
    print(f"  Tool_Age_Years range        : {age_years.min():.2f} → {age_years.max():.2f}")
    print(f"  Tool_Age_Years sample (first 10):")
    print(age_years.head(10).to_string())

    # 4) Age bucket distribution across ALL MOM tools (active + inactive)
    def bucket(y):
        if pd.isna(y):   return "UnknownAge"
        elif y < 1:      return "0_1Y"
        elif y < 3:      return "1_3Y"
        elif y < 5:      return "3_5Y"
        else:            return "5P_Y"

    buckets = age_years.apply(bucket)
    print(f"
  Age bucket distribution (all MOM tools):")
    print(buckets.value_counts().to_string())

    # 5) install_sts distribution — shows what values are actually present
    print(f"
  install_sts value counts (top 20):")
    print(df_mom["install_sts"].value_counts().head(20).to_string())

    # 6) Active count using current INACTIVE_STATUSES
    active_mask = is_active(df_mom["install_sts"])
    print(f"
  Active tools (not in INACTIVE_STATUSES) : {active_mask.sum():,}")
    print(f"  Inactive tools                           : {(~active_mask).sum():,}")
    print(f"  INACTIVE_STATUSES used                   : {INACTIVE_STATUSES}")

    # 7) Age bucket distribution for ACTIVE tools only
    active_buckets = buckets[active_mask]
    print(f"
  Age bucket distribution (ACTIVE tools only):")
    print(active_buckets.value_counts().to_string())

    print("=" * 60 + "
")
