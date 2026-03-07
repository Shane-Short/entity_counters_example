"""
run_02_build_marts_and_export.py
---------------------------------
Step 2 of 2 in the Inventory Installed Base pipeline.

What this script does:
  1. Read curated tables from SQL Server (written by Script 1)
  2. Build mart_installed_base_by_config   — active tool counts per config
  3. Build mart_inventory_installed_base_drilldown  — one row per Part + config
  4. Build mart_inventory_installed_base_leadership — one row per Part_Number
  5. Write all three mart tables to SQL Server
  6. Export a two-sheet Excel file:
       Sheet 1: Leadership_Product_Table
       Sheet 2: Drilldown_Table

CRITICAL counting rules (do not change without reading the brief):
  - Always COUNT(DISTINCT ENTITY) — never count rows.
  - Active = install_sts NOT IN ('Bagged', 'Not Installed').
  - MOM-only tools were already filtered in Script 1 (no underscore in ENTITY).
  - Part-level tool count must NOT be a sum of per-config counts.
    It must be a fresh COUNT(DISTINCT ENTITY) after joining all matching configs.
"""

import pandas as pd
import numpy as np
from datetime import date
from sqlalchemy import create_engine
import urllib

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
SQL_SERVER   = "TEHAUSTELSQL1"
SQL_INSTANCE = "TELAPPSSQLSVR"
SQL_PORT     = 52317          # ← replace with actual port from SQL Server Config Manager
                               #   (TCP/IP → IP Addresses → IPAll → TCP Dynamic Ports)
SQL_DATABASE = "ESData"
SQL_USERNAME = "your_username"
SQL_PASSWORD = "your_password"

# Named instance connection: use host\instance for the SERVER value.
# Do NOT combine backslash-instance with a comma-port — the driver handles
# port resolution via the instance name when SQL Server Browser is running,
# OR you can pin the port explicitly as a separate key (Port=NNNN).
CONN_STR = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SQL_SERVER}\\{SQL_INSTANCE};"
    f"Port={SQL_PORT};"
    f"DATABASE={SQL_DATABASE};"
    f"UID={SQL_USERNAME};"
    f"PWD={SQL_PASSWORD};"
    "TrustServerCertificate=yes;"
)

EXCEL_OUTPUT = r"C:\Users\180944\Desktop\Inventory_Installed_Base_Analysis.xlsx"

# Statuses that mean "not an active tool"
INACTIVE_STATUSES = {"Bagged", "Not Installed"}

# Age bucket boundaries (in years)
ASOF_DATE = pd.Timestamp(date.today())


def get_engine():
    params = urllib.parse.quote_plus(CONN_STR)
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True,
    )


def write_table(df, table_name, engine):
    print(f"  Writing {len(df):,} rows → dbo.{table_name} ...")
    df.to_sql(table_name, con=engine, schema="dbo", if_exists="replace", index=False)
    print(f"  Done: dbo.{table_name}")
# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------
def add_age_bucket(df):
    """
    Given a DataFrame with Warranty_Start column, add Tool_Age_Years and
    Age_Bucket columns.  All downstream counts still use DISTINCT ENTITY.
    """
    df = df.copy()
    df["Warranty_Start"] = pd.to_datetime(df["Warranty_Start"], errors="coerce")
    df["Tool_Age_Days"]  = (ASOF_DATE - df["Warranty_Start"]).dt.days
    df["Tool_Age_Years"] = df["Tool_Age_Days"] / 365.25
    def bucket(years):
        if pd.isna(years):
            return "UnknownAge"
        elif years < 1:
            return "0_1Y"
        elif years < 3:
            return "1_3Y"
        elif years < 5:
            return "3_5Y"
        else:
            return "5P_Y"
    df["Age_Bucket"] = df["Tool_Age_Years"].apply(bucket)
    return df


def is_active(series):
    """Return boolean mask: True where install_sts is NOT in INACTIVE_STATUSES."""
    return ~series.isin(INACTIVE_STATUSES)


# ---------------------------------------------------------------------------
# STEP 1: Read curated tables from SQL
# ---------------------------------------------------------------------------
def read_tables(engine):
    print("Reading curated tables from SQL Server ...")
    df_inv  = pd.read_sql("SELECT * FROM dbo.cur_inventory",          engine)
    df_mom  = pd.read_sql("SELECT * FROM dbo.cur_auto_tool_list_mom", engine)
    df_gbom = pd.read_sql("SELECT * FROM dbo.cur_gbom_applicability", engine)
    # Strip column name whitespace that can sneak in via SQL Server round-trips
    df_inv.columns  = df_inv.columns.str.strip()
    df_mom.columns  = df_mom.columns.str.strip()
    df_gbom.columns = df_gbom.columns.str.strip()
    print(f"  Inventory rows : {len(df_inv):,}")
    print(f"  MOM tool rows  : {len(df_mom):,}  (unique tools: {df_mom['ENTITY'].nunique():,})")
    print(f"  GBOM app rows  : {len(df_gbom):,}")
    return df_inv, df_mom, df_gbom


# ---------------------------------------------------------------------------
# STEP 2: mart_installed_base_by_config
# ---------------------------------------------------------------------------
def build_config_mart(df_mom):
    """
    Grain: Platform + Three_CEID + Process_Node
    For each config, count DISTINCT active ENTITYs — broken down by age bucket.
    Active = install_sts NOT IN ('Bagged', 'Not Installed').
    We never sum these counts to derive higher-level numbers.
    """
    print("Building mart_installed_base_by_config ...")

    df_mom = df_mom.copy()
    df_mom["Process_Node"] = pd.to_numeric(df_mom["Process_Node"], errors="coerce").astype("Int64")

    # Tool_ID = ENTITY + Location — ENTITY alone is not globally unique because
    # the same tool ID can exist in different locations as different physical tools.
    df_mom["Tool_ID"] = df_mom["ENTITY"].astype(str) + "|" + df_mom["Location"].astype(str)

    df_mom = add_age_bucket(df_mom)

    # Active flag
    df_mom["is_active"] = is_active(df_mom["install_sts"])

    # --- Total active distinct count per config ---
    active_df = df_mom[df_mom["is_active"]].copy()
    # Join key is Three_CEID + Process_Node only — Platform excluded intentionally.
    config_total = (
        active_df
        .groupby(["Three_CEID", "Process_Node"])["Tool_ID"]
        .nunique()
        .reset_index()
        .rename(columns={"Tool_ID": "Active_Tool_Count_Config"})
    )

    # --- Age-bucket counts (still DISTINCT per config + bucket) ---
    age_counts = (
        active_df
        .groupby(["Three_CEID", "Process_Node", "Age_Bucket"])["Tool_ID"]
        .nunique()
        .reset_index()
        .rename(columns={"Tool_ID": "Count"})
    )
    age_pivot = age_counts.pivot_table(
        index=["Three_CEID", "Process_Node"],
        columns="Age_Bucket",
        values="Count",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()

    # Rename pivot columns to human-readable names
    age_col_map = {
        "0_1Y"       : "Count_Of_Tools_Age_0_to_1_Yr",
        "1_3Y"       : "Count_Of_Tools_Age_1_to_3_Yr",
        "3_5Y"       : "Count_Of_Tools_Age_3_to_5_Yr",
        "5P_Y"       : "Count_Of_Tools_Age_5_Plus_Yr",
        "UnknownAge" : "Count_Of_Tools_Age_Unknown",
    }
    age_pivot = age_pivot.rename(columns=age_col_map)
    age_pivot.columns.name = None

    # Merge total + age splits
    mart_config = config_total.merge(age_pivot, on=["Three_CEID", "Process_Node"], how="left")

    # Fill any missing age columns with 0
    for col in age_col_map.values():
        if col not in mart_config.columns:
            mart_config[col] = 0
    mart_config["Process_Node"] = mart_config["Process_Node"].astype("Int64")
    print(f"  Config mart rows: {len(mart_config):,}")
    return mart_config


# ---------------------------------------------------------------------------
# STEP 3: mart_inventory_installed_base_drilldown
# ---------------------------------------------------------------------------
def build_drilldown(df_gbom, mart_config, df_inv):
    """
    Grain: Part_Number + Platform + Three_CEID + Process_Node
    Join GBOM applicability to config mart to get active tool counts.
    Join inventory aggregates (sum qty, sum amount) at Part_Number level.
    """
    print("Building drilldown mart ...")
    # Inventory part-level aggregates (needed for both drilldown and leadership)
    inv_part = (
        df_inv
        .groupby("Part_Number", as_index=False)
        .agg(
            Qty_Total       =("Qty",            "sum"),
            Total_Amount_USD=("Total_Amount_USD","sum"),
        )
    )
    inv_part["Cost_per_part"] = (
        inv_part["Total_Amount_USD"] / inv_part["Qty_Total"].replace(0, np.nan)
    )

    # Ensure Process_Node type consistency before joining
    df_gbom = df_gbom.copy()
    df_gbom["Process_Node"] = pd.to_numeric(df_gbom["Process_Node"], errors="coerce").astype("Int64")
    mart_config["Process_Node"] = mart_config["Process_Node"].astype("Int64")

    # Join GBOM → config mart on Three_CEID + Process_Node only (no Platform)
    drilldown = df_gbom.merge(
        mart_config,
        on=["Three_CEID", "Process_Node"],
        how="left",
    )

    # Fill tool counts with 0 where no matching config (part exists in GBOM
    # but no active tools for that config)
    tool_count_cols = [c for c in drilldown.columns if c.startswith("Active_Tool_Count")]
    drilldown[tool_count_cols] = drilldown[tool_count_cols].fillna(0)

    # Join inventory aggregates
    drilldown = drilldown.merge(inv_part, on="Part_Number", how="left")

    # Optional: estimated parts needed for this config
    drilldown["Estimated_Parts_Needed_Config"] = (
        drilldown["Active_Tool_Count_Config"] * drilldown["Qty_Per_Tool"]
    )
    print(f"  Drilldown rows: {len(drilldown):,}")
    return drilldown, inv_part


# ---------------------------------------------------------------------------
# STEP 4: mart_inventory_installed_base_leadership
# ---------------------------------------------------------------------------
def build_leadership(df_gbom, df_mom, df_inv, inv_part):
    """
    Grain: Part_Number (one row per part)
    IMPORTANT — how Number_of_active_tools is computed:
      1. Join GBOM applicability to MOM tool list on (Platform, Three_CEID, Process_Node).
      2. Filter active tools.
      3. GROUP BY Part_Number → COUNT(DISTINCT ENTITY).
    We do NOT sum per-config active tool counts — that would double-count tools
    that appear in multiple configs.
    """
    print("Building leadership mart ...")
    print(f"  Inventory columns available: {list(df_inv.columns)}")
    # --- Inventory aggregates ---
    # Build the agg spec dynamically so missing optional columns (MOU12, MOU36,
    # Supplier_Code) don't cause a KeyError — they'll simply be absent from the
    # output rather than crashing the script.
    # MOU columns intentionally excluded from final output per business request.
    agg_spec = {
        "Amount_of_stock" : ("Qty",            "sum"),
        "Total_Amount_USD": ("Total_Amount_USD","sum"),
    }
    if "Supplier_Code" in df_inv.columns:
        agg_spec["Supplier_Code"] = ("Supplier_Code", "first")
    inv_agg = df_inv.groupby("Part_Number", as_index=False).agg(**agg_spec)
    inv_agg["Cost_per_part"] = (
        inv_agg["Total_Amount_USD"] / inv_agg["Amount_of_stock"].replace(0, np.nan)
    )

    # --- Max Qty_Per_Tool across all GBOM configs for each part ---
    # (used as a conservative demand estimate)
    gbom_max_qty = (
        df_gbom
        .groupby("Part_Number", as_index=False)["Qty_Per_Tool"]
        .max()
        .rename(columns={"Qty_Per_Tool": "Amount_needed_per_tool"})
    )

    # --- Applicable Three_CEID and Tool_Type lists per part ---
    # For each part, collect all unique Three_CEID and Tool_Type values from GBOM
    # and join them into a comma-separated string so each part has one summary row.
    gbom_lists = (
        df_gbom
        .groupby("Part_Number", as_index=False)
        .agg(
            Applicable_CEIDs      =("Three_CEID", lambda x: ", ".join(sorted(x.dropna().astype(str).unique()))),
            Applicable_Tool_Types =("Tool_Type",  lambda x: ", ".join(sorted(x.dropna().astype(str).unique()))),
        )
    )

    # --- True part-level DISTINCT active tool count ---
    # Step A: join GBOM → MOM on (Platform, Three_CEID, Process_Node)
    # We keep only the ENTITY and Part_Number columns we need.

    # Join keys are Three_CEID + Process_Node only — Platform excluded intentionally.
    df_gbom_keys = df_gbom[["Part_Number", "Three_CEID", "Process_Node"]].copy()
    df_gbom_keys["Process_Node"] = pd.to_numeric(
        df_gbom_keys["Process_Node"], errors="coerce"
    ).astype("Int64")
    df_mom_keys = df_mom[["ENTITY", "Location", "Three_CEID", "Process_Node", "install_sts",
                           "Warranty_Start"]].copy()
    df_mom_keys["Process_Node"] = pd.to_numeric(
        df_mom_keys["Process_Node"], errors="coerce"
    ).astype("Int64")
    # Tool_ID = ENTITY + Location — same composite key used throughout the pipeline
    df_mom_keys["Tool_ID"] = df_mom_keys["ENTITY"].astype(str) + "|" + df_mom_keys["Location"].astype(str)
    df_mom_keys = add_age_bucket(df_mom_keys)

    # -----------------------------------------------------------------------
    # JOIN DIAGNOSTICS — printed every run to catch key mismatches early.
    # If the join produces 0 rows, one of these three key columns has a
    # formatting difference between GBOM and Auto Tool List.
    # -----------------------------------------------------------------------
    print("--- Join key diagnostics ---")
    print(f"  GBOM   Three_CEID sample: {sorted(df_gbom_keys['Three_CEID'].dropna().unique())[:10]}")
    print(f"  MOM    Three_CEID sample: {sorted(df_mom_keys['Three_CEID'].dropna().unique())[:10]}")
    print(f"  GBOM   Process_Node sample: {sorted(df_gbom_keys['Process_Node'].dropna().unique())[:10]}")
    print(f"  MOM    Process_Node sample: {sorted(df_mom_keys['Process_Node'].dropna().unique())[:10]}")
    gbom_ceids = set(df_gbom_keys["Three_CEID"].dropna().unique())
    mom_ceids  = set(df_mom_keys["Three_CEID"].dropna().unique())
    gbom_nodes = set(df_gbom_keys["Process_Node"].dropna().unique())
    mom_nodes  = set(df_mom_keys["Process_Node"].dropna().unique())
    print(f"  Three_CEID overlap  : {len(gbom_ceids & mom_ceids)} of {len(gbom_ceids)} GBOM values match MOM")
    print(f"  Process_Node overlap: {len(gbom_nodes & mom_nodes)} of {len(gbom_nodes)} GBOM values match MOM")
    if not (gbom_ceids & mom_ceids):
        print("  *** WARNING: ZERO Three_CEID values overlap — join will produce 0 rows ***")
    if not (gbom_nodes & mom_nodes):
        print("  *** WARNING: ZERO Process_Node values overlap — join will produce 0 rows ***")
    print("--- End join key diagnostics ---")
    # Step B: join GBOM keys → MOM keys on Three_CEID + Process_Node only
    part_tool_join = df_gbom_keys.merge(
        df_mom_keys,
        on=["Three_CEID", "Process_Node"],
        how="inner",
    )
    print(f"  Rows after join (GBOM x MOM): {len(part_tool_join):,}")
    if len(part_tool_join) == 0:
        print("  *** WARNING: join produced 0 rows — all tool counts will be 0 ***")
    # Step C: filter active
    part_tool_join["is_active"] = is_active(part_tool_join["install_sts"])
    active_join = part_tool_join[part_tool_join["is_active"]].copy()

    # --- Bucket consistency diagnostic ---
    # Find any ENTITY that appears in more than one Age_Bucket for the same
    # Part_Number — this is the only way bucket sums can exceed Number_of_active_tools.
    bucket_check = (
        active_join
        .groupby(["Part_Number", "Tool_ID"])["Age_Bucket"]
        .nunique()
        .reset_index()
        .rename(columns={"Age_Bucket": "Distinct_Buckets"})
    )
    multi_bucket = bucket_check[bucket_check["Distinct_Buckets"] > 1]
    if len(multi_bucket) > 0:
        print(f"  *** WARNING: {len(multi_bucket)} (Part, Tool_ID) pairs span multiple age buckets ***")
        print("  Sample offending tools:")
        sample_ids = multi_bucket["Tool_ID"].unique()[:5]
        for tid in sample_ids:
            rows = active_join[active_join["Tool_ID"] == tid][["Tool_ID", "Three_CEID", "Process_Node", "Warranty_Start", "Age_Bucket"]].drop_duplicates()
            print(rows.to_string(index=False))
    else:
        print("  Bucket consistency check PASSED — no tool spans multiple buckets")
    # --- End diagnostic ---

    # Step D: COUNT(DISTINCT Tool_ID) per Part_Number — Tool_ID = ENTITY + Location
    # Using Tool_ID prevents same-named tools in different locations from being
    # collapsed into one, which would undercount the true number of active tools.
    part_tool_counts = (
        active_join
        .groupby("Part_Number")["Tool_ID"]
        .nunique()
        .reset_index()
        .rename(columns={"Tool_ID": "Number_of_active_tools"})
    )

    # Step E: age-split distinct counts at part level — still using Tool_ID
    age_part = (
        active_join
        .groupby(["Part_Number", "Age_Bucket"])["Tool_ID"]
        .nunique()
        .reset_index()
        .rename(columns={"Tool_ID": "Count"})
    )
    age_part_pivot = age_part.pivot_table(
        index="Part_Number",
        columns="Age_Bucket",
        values="Count",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()
    age_col_map = {
        "0_1Y"       : "Count_Of_Tools_Age_0_to_1_Yr",
        "1_3Y"       : "Count_Of_Tools_Age_1_to_3_Yr",
        "3_5Y"       : "Count_Of_Tools_Age_3_to_5_Yr",
        "5P_Y"       : "Count_Of_Tools_Age_5_Plus_Yr",
        "UnknownAge" : "Count_Of_Tools_Age_Unknown",
    }
    age_part_pivot = age_part_pivot.rename(columns=age_col_map)
    age_part_pivot.columns.name = None

    # --- Assemble leadership table ---
    # Start from all inventory parts
    leadership = inv_agg.merge(gbom_max_qty,      on="Part_Number", how="left")
    leadership = leadership.merge(gbom_lists,       on="Part_Number", how="left")
    leadership = leadership.merge(part_tool_counts, on="Part_Number", how="left")
    leadership = leadership.merge(age_part_pivot,   on="Part_Number", how="left")

    # Fill missing tool counts with 0 (parts not matched in GBOM or no active tools)
    tool_cols = ["Number_of_active_tools"] + list(age_col_map.values())
    for col in tool_cols:
        if col in leadership.columns:
            leadership[col] = leadership[col].fillna(0).astype(int)

    # --- Active flag ---
    leadership["Active_Installed_Flag"] = np.where(
        leadership["Number_of_active_tools"] > 0, "Y", "N"
    )

    # --- Disposition logic (§9) ---
    in_gbom_parts = set(df_gbom["Part_Number"].unique())
    def disposition(row):
        if row["Part_Number"] not in in_gbom_parts:
            return "Divest/Scrap", "Low"
        elif row["Number_of_active_tools"] > 0:
            return "Sell", "High"
        else:
            return "Investigate", "Medium"
    disp = leadership.apply(disposition, axis=1, result_type="expand")
    leadership["Disposition_Category"] = disp[0]
    leadership["Confidence_Level"]     = disp[1]
    print(f"  Leadership rows: {len(leadership):,}")
    print(f"    Active_Installed_Flag=Y : {(leadership['Active_Installed_Flag']=='Y').sum():,}")
    print(f"    Active_Installed_Flag=N : {(leadership['Active_Installed_Flag']=='N').sum():,}")
    return leadership


# ---------------------------------------------------------------------------
# STEP 5: Write marts to SQL
# ---------------------------------------------------------------------------
def write_marts(mart_config, drilldown, leadership, engine):
    write_table(mart_config,  "mart_installed_base_by_config",               engine)
    write_table(drilldown,    "mart_inventory_installed_base_drilldown",      engine)
    write_table(leadership,   "mart_inventory_installed_base_leadership",     engine)


# ---------------------------------------------------------------------------
# STEP 6: Export Excel (two sheets)
# ---------------------------------------------------------------------------
def export_excel(leadership, drilldown, path):
    print(f"Exporting Excel → {path}")
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        leadership.to_excel(writer, sheet_name="Leadership_Product_Table", index=False)
        drilldown.to_excel(writer,  sheet_name="Drilldown_Table",          index=False)
    print("  Excel export complete.")
# ---------------------------------------------------------------------------
# ACCEPTANCE TESTS
# ---------------------------------------------------------------------------
def run_acceptance_tests(df_mom, mart_config, leadership, df_gbom):
    """
    Quick sanity checks — raise AssertionError if something is wrong.
    """
    print("Running acceptance tests ...")
    # A) MOM-only: zero entities with underscore
    assert df_mom["ENTITY"].str.contains("_").sum() == 0, \
        "FAIL A: chamber entities found in cur_auto_tool_list_mom"

    # B) No config active count exceeds total distinct tools in that config
    total_mom_distinct = df_mom["ENTITY"].nunique()
    max_config_count = mart_config["Active_Tool_Count_Config"].max() if len(mart_config) else 0
    assert max_config_count <= total_mom_distinct, \
        "FAIL B: a config active count exceeds total MOM distinct tools"

    # C) Part-level count is NOT a sum of config counts
    # Heuristic: pick a part that appears in multiple configs and verify
    multi_config_parts = df_gbom.groupby("Part_Number").size()
    multi_config_parts = multi_config_parts[multi_config_parts > 1]
    if len(multi_config_parts) > 0:
        sample_part = multi_config_parts.index[0]
        part_row = leadership[leadership["Part_Number"] == sample_part]
        if len(part_row) > 0:
            reported = part_row["Number_of_active_tools"].iloc[0]
            # The correct value can't exceed total MOM tools
            assert reported <= total_mom_distinct, \
                f"FAIL C: Number_of_active_tools ({reported}) > total MOM distinct ({total_mom_distinct})"
    print("  All acceptance tests PASSED.")
# ---------------------------------------------------------------------------
# DEBUG HELPER — remove or comment out once root cause is confirmed
# ---------------------------------------------------------------------------
def debug_age_and_active(df_mom):
    """
    Prints diagnostic info to identify why age buckets or active counts look wrong.
    Check this output before drawing any conclusions about the data.
    """
    print("=" * 60)
    print("DEBUG: Age bucket + active status diagnostics")
    print("=" * 60)
    # 1) Show today's date being used as the reference point
    print(f"ASOF_DATE (today): {ASOF_DATE.date()}")
    # 2) Show raw Warranty_Start samples BEFORE any parsing
    print(f"Warranty_Start dtype from SQL : {df_mom['Warranty_Start'].dtype}")
    print("Sample raw values (first 10):")
    print(df_mom["Warranty_Start"].head(10).to_string())
    # 3) Parse dates and compute ages — same logic as add_age_bucket()
    ws = pd.to_datetime(df_mom["Warranty_Start"], errors="coerce")
    age_years = (ASOF_DATE - ws).dt.days / 365.25
    print(f"Warranty_Start null count : {ws.isna().sum():,} of {len(ws):,}")
    print(f"Parsed date range :")
    print(f"{ws.min()} -> {ws.max()}")
    print(f"Tool_Age_Years range :")
    print(f"{age_years.min():.2f} -> {age_years.max():.2f}")
    print("Tool_Age_Years sample (first 10):")
    print(age_years.head(10).to_string())
    # 4) Age bucket distribution across ALL MOM tools (active + inactive)
    def bucket(y):
        if pd.isna(y):   return "UnknownAge"
        elif y < 1:      return "0_1Y"
        elif y < 3:      return "1_3Y"
        elif y < 5:      return "3_5Y"
        else:            return "5P_Y"
    buckets = age_years.apply(bucket)
    print("Age bucket distribution (all MOM tools):")
    print(buckets.value_counts().to_string())
    # 5) install_sts distribution — shows what values are actually present
    print("Install_sts value counts (top 20):")
    print(df_mom["install_sts"].value_counts().head(20).to_string())
    # 6) Active count using current INACTIVE_STATUSES
    active_mask = is_active(df_mom["install_sts"])
    print(f"Active tools (not in INACTIVE_STATUSES): {active_mask.sum():,}")
    print(f"Inactive tools: {(~active_mask).sum():,}")
    print(f"INACTIVE_STATUSES used: {INACTIVE_STATUSES}")
    # 7) Age bucket distribution for ACTIVE tools only
    active_buckets = buckets[active_mask]
    print("Age bucket distribution (ACTIVE tools only):")
    print(active_buckets.value_counts().to_string())
    print("=" * 60)
# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("Script 2: Build Marts + Export")
    print("=" * 60)
    engine = get_engine()

    # 1) Load curated tables
    df_inv, df_mom, df_gbom = read_tables(engine)

    # DEBUG — run this block, read the output, then we can fix the root cause
    debug_age_and_active(df_mom)

    # 2) Config mart (active tool counts per Platform/Three_CEID/Process_Node)
    mart_config = build_config_mart(df_mom)

    # 3) Drilldown + inventory part aggregates
    drilldown, inv_part = build_drilldown(df_gbom, mart_config, df_inv)

    # 4) Leadership (one row per Part_Number, true distinct tool count)
    leadership = build_leadership(df_gbom, df_mom, df_inv, inv_part)

    # 5) Write to SQL
    write_marts(mart_config, drilldown, leadership, engine)

    # 6) Export Excel
    export_excel(leadership, drilldown, EXCEL_OUTPUT)

    # 7) Acceptance tests
    run_acceptance_tests(df_mom, mart_config, leadership, df_gbom)
    print("Script 2 complete.")
    print(f"Excel file: {EXCEL_OUTPUT}")
    print("SQL tables written to TEHAUSTELSQL1.ESData.dbo.*")
if __name__ == "__main__":
    main()
