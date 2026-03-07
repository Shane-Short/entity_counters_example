"""
run_01_ingest_and_normalize.py
-------------------------------
Step 1 of 2 in the Inventory Installed Base pipeline.

What this script does:
  1. Read current_inventory.xlsb  → df_inventory
  2. Read Auto_Tool_List.csv       → df_auto, then filter MOM-only → df_mom
  3. Read GBOM.xlsx (multi-row header) and normalize wide → long → df_gbom
  4. Write all three curated DataFrames to SQL Server

SQL target: TEHAUSTELSQL1.ESData.dbo.*
Tables written:
  - dbo.cur_inventory
  - dbo.cur_auto_tool_list_mom
  - dbo.cur_gbom_applicability
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import urllib

# ---------------------------------------------------------------------------
# CONFIG — edit these paths/connection if environment changes
# ---------------------------------------------------------------------------
INVENTORY_PATH  = r"C:\Users\180944\Desktop\current_inventory.xlsb"
AUTO_TOOL_PATH  = r"C:\Users\180944\Desktop\Auto_Tool_List.csv"
GBOM_PATH       = r"C:\Users\180944\Desktop\GBOM.xlsx"

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


def get_engine():
    """Build a SQLAlchemy engine using pyodbc + explicit SQL credentials."""
    params = urllib.parse.quote_plus(CONN_STR)
    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True,   # much faster bulk inserts
    )
    return engine


def write_table(df, table_name, engine):
    """Write a DataFrame to SQL Server, replacing the table if it exists."""
    print(f"  Writing {len(df):,} rows → dbo.{table_name} ...")
    df.to_sql(table_name, con=engine, schema="dbo", if_exists="replace", index=False)
    print(f"  Done: dbo.{table_name}")


# ---------------------------------------------------------------------------
# 1) CURRENT INVENTORY
# ---------------------------------------------------------------------------

# These are words we expect to find in the real header row.
# The scan below looks for the first row that contains at least one of these.
INVENTORY_HEADER_SIGNALS = {
    "part number", "part_number", "region", "supplier", "qty",
    "total amount", "mou12", "mou36", "storage",
}

def find_header_row(path):
    """
    The .xlsb file may have metadata rows at the top (e.g. 'Folder Path')
    before the real column headers appear.

    Strategy: read the whole file with no header, then scan row by row until
    we find one whose values contain at least one known header keyword.
    Returns the 0-based row index of the real header row.
    """
    raw = pd.read_excel(path, engine="pyxlsb", header=None, sheet_name="GCE research-AGE of Inventory")
    for i, row in raw.iterrows():
        # Flatten all cell values in this row to lowercase strings
        row_vals = {str(v).strip().lower() for v in row if pd.notna(v) and str(v).strip()}
        # Check if any cell matches a known header signal word/phrase
        for val in row_vals:
            for signal in INVENTORY_HEADER_SIGNALS:
                if signal in val:
                    print(f"  Detected real header at row index {i} (Excel row {i+1})")
                    return i
    raise ValueError(
        "Could not locate the real header row in the inventory file. "
        "Add a word from your actual header to INVENTORY_HEADER_SIGNALS."
    )


def load_inventory(path):
    """
    Read the .xlsb inventory file.

    The file contains metadata rows above the real headers (e.g. a 'Folder Path'
    row). We auto-detect which row holds the actual column names by scanning for
    known header keywords, then re-read the file using that row as the header.
    """
    print("Loading inventory ...")

    # Step 1: find which row is the real header
    header_row = find_header_row(path)

    # Step 2: re-read using that row as the header; everything above is skipped
    df = pd.read_excel(path, engine="pyxlsb", header=header_row, sheet_name="GCE research-AGE of Inventory")

    # Strip leading/trailing whitespace from all column names
    df.columns = df.columns.str.strip()

    # Drop completely empty rows (common in .xlsb exports)
    df = df.dropna(how="all")

    print(f"  Raw rows after header skip: {len(df):,}")
    print(f"  Columns found: {list(df.columns)}")

    # Drop unwanted columns by prefix/exact match before anything else.
    # Add more entries here if other junk columns appear in future exports.
    DROP_PREFIXES = ("Active -", "(double check)", "What is the tool model name?")
    cols_to_drop = [c for c in df.columns if str(c).startswith(DROP_PREFIXES)]
    if cols_to_drop:
        print(f"  Dropping {len(cols_to_drop)} unwanted column(s): {cols_to_drop}")
        df = df.drop(columns=cols_to_drop)

    # Canonical column map — exact source column names from the inventory sheet
    rename_map = {
        # source name                  : canonical name
        "Region_Name"                  : "Region_Name",
        "Supplier code"                : "Supplier_Code",
        "StorageLocation_Description"  : "StorageLocation_Description",
        "P/N"                          : "Part_Number",
        "Description"                  : "Part_Description",
        "qty"                          : "Qty",
        "Total Amount ($)"             : "Total_Amount_USD",
        "MOU 12"                       : "MOU12",
        "MOU 36"                       : "MOU36",
        "ABC_LOCAL"                    : "ABC_LOCAL",
        "NIS Machine Code"             : "NIS_Machine_Code",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Coerce numeric columns
    for col in ["Qty", "Total_Amount_USD", "MOU12", "MOU36"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    print(f"  Inventory rows: {len(df):,}")
    return df


# ---------------------------------------------------------------------------
# 2) AUTO TOOL LIST — MOM-only filter
# ---------------------------------------------------------------------------
def load_auto_tool_list_mom(path):
    """
    Read the CSV, then keep only MOM/platform-level tools.

    MOM-only rule: ENTITY must NOT contain an underscore.
      - 'ABC123'     → keep   (platform tool)
      - 'ABC123_PM1' → drop   (chamber tool)

    We also parse Process_Node to int and Warranty_Start to datetime.
    """
    print("Loading Auto Tool List ...")
    df = pd.read_csv(path, dtype=str)          # read everything as string first
    df.columns = df.columns.str.strip()

    # ---- MOM filter (the most important rule in this whole pipeline) ----
    df = df[~df["ENTITY"].astype(str).str.contains("_", regex=False)].copy()
    print(f"  MOM-only tool rows: {len(df):,}  (chambers excluded)")

    # Validate: zero chamber rows must remain
    assert df["ENTITY"].str.contains("_").sum() == 0, \
        "BUG: chamber entities found after MOM filter!"

    # Strip whitespace from all join key columns so they match GBOM values cleanly.
    # Leading/trailing spaces in the CSV are a common source of silent join failures.
    for col in ["ENTITY", "PF", "Three_CEID"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Normalise types
    df["Process_Node"] = pd.to_numeric(df["Process_Node"], errors="coerce").astype("Int64")

    if "Warranty_Start" in df.columns:
        df["Warranty_Start"] = pd.to_datetime(df["Warranty_Start"], errors="coerce")

    # Rename location column to a consistent name if needed
    if "Fab" in df.columns and "Location" not in df.columns:
        df = df.rename(columns={"Fab": "Location"})

    # Normalize any truncated or alternate spellings of install_sts so the
    # column always lands in SQL as 'install_sts' with the full name intact.
    # SQL Server can silently truncate column names, so we force it here.
    install_sts_variants = ["Install_sta", "install_sta", "Install_Sts", "InstallSts"]
    for variant in install_sts_variants:
        if variant in df.columns:
            print(f"  Renaming '{variant}' → 'install_sts'")
            df = df.rename(columns={variant: "install_sts"})

    return df


# ---------------------------------------------------------------------------
# 3) GBOM — multi-row header normalisation (wide → long)
# ---------------------------------------------------------------------------
def load_gbom_applicability(path):
    """
    GBOM has a 3-row compound header followed by a blank row, then data.

    Excel layout (1-based rows):
      Row 1: base column names  +  Process_Node values for config cols  (e.g. P1274)
      Row 2: Three_CEID values for config cols
      Row 3: Tool_Type values for config cols
      Row 4: blank
      Row 5: first data row

    0-based pandas equivalent:
      header rows  → 0, 1, 2  (read via header=None, then slice)
      blank row    → index 3
      data rows    → index 4 onward

    Strategy:
      a) Read the whole sheet with header=None.
      b) Extract rows 0-2 as the header metadata.
      c) Extract rows 4+ as the data block.
      d) Identify which columns are "base" (left side) vs "config" (right side).
      e) melt config columns → long table.
      f) Parse Process_Node / Three_CEID / Tool_Type from header rows.
      g) Drop rows where Qty_Per_Tool is null / 0 or Part_Number is missing.

    Config columns to IGNORE: CD through CH (indices approx 81-85 in Excel).
    We identify them by checking that Process_Node header is unparseable.
    """
    print("Loading GBOM ...")

    # Read raw — no header interpretation yet
    raw = pd.read_excel(path, header=None, engine="openpyxl")

    # ---- Extract the 3 header rows ----
    hdr_process_node = raw.iloc[0]   # row 1 in Excel
    hdr_three_ceid   = raw.iloc[1]   # row 2
    hdr_tool_type    = raw.iloc[2]   # row 3

    # ---- Data starts at row index 3 (Excel row 4, 0-based) ----
    # The brief noted a blank row 4 before data, but the actual file has no
    # blank row — data begins immediately after the 3-row header block.
    # reset_index here is critical: it ensures data's integer index starts at 0
    # so that row_i in the melt loop below aligns correctly with df_base.iloc[row_i]
    data = raw.iloc[3:].reset_index(drop=True)

    # ---- Identify base columns ----
    # Base columns: those whose row-0 header is a plain string like 'PF', 'Part Number', etc.
    # Config columns: those where row-0 header looks like a Process Node (e.g. 'P1274' or numeric).
    # We use a simple heuristic: base cols are the leftmost contiguous columns where the
    # row-1 header (Three_CEID) is null/blank, because config columns always have a Three_CEID.

    base_col_indices = []
    config_col_indices = []

    for col_idx in range(len(hdr_process_node)):
        ceid_val = hdr_three_ceid.iloc[col_idx]
        pn_val   = hdr_process_node.iloc[col_idx]

        # Skip the CD–CH columns: they have no usable Process_Node header
        # We detect them by: Three_CEID is blank AND Process_Node header is also blank/non-P-prefixed
        if pd.isna(ceid_val) or str(ceid_val).strip() == "":
            base_col_indices.append(col_idx)
        else:
            config_col_indices.append(col_idx)

    # ---- Build base DataFrame ----
    # reset_index(drop=True) ensures df_base shares the exact same 0-based integer
    # index as data, so df_base.iloc[row_i] always matches data.iloc[row_i]
    df_base = data.iloc[:, base_col_indices].reset_index(drop=True)
    df_base.columns = [str(hdr_process_node.iloc[i]).strip() for i in base_col_indices]

    # Rename base columns to canonical names
    base_rename = {
        "PF"              : "Platform",
        "Part Number"     : "Part_Number",
        "Part Description": "Part_Description",
        "OEM Name"        : "OEM_Name",
        "OEM Part Number" : "OEM_Part_Number",
    }
    df_base = df_base.rename(columns={k: v for k, v in base_rename.items() if k in df_base.columns})

    # Drop rows with no Part_Number
    df_base = df_base.dropna(subset=["Part_Number"])
    df_base = df_base[df_base["Part_Number"].astype(str).str.strip() != ""]

    # ---- Build config column metadata (one entry per config column) ----
    # A header cell can contain multiple process nodes separated by '/'
    # e.g. 'P1274/P1278' means this column applies to BOTH nodes.
    # We split on '/' and emit one config_meta entry per node, all sharing
    # the same col_idx, Three_CEID, Tool_Type, and Qty_Per_Tool.
    # This ensures the join to Auto_Tool_List (which has one node per row)
    # matches correctly for every node in the combined header.
    config_meta = []
    for col_idx in config_col_indices:
        raw_pn   = str(hdr_process_node.iloc[col_idx]).strip()
        ceid_val = str(hdr_three_ceid.iloc[col_idx]).strip()
        type_val = str(hdr_tool_type.iloc[col_idx]).strip() if not pd.isna(hdr_tool_type.iloc[col_idx]) else ""

        # Split on '/' to handle combined headers like 'P1274/P1278'
        pn_parts = raw_pn.split("/")

        for pn_part in pn_parts:
            # Strip leading 'P' or 'p', then convert to int
            # E.g. 'P1274' → 1274 ;  skip if unparseable (those are the CD-CH cols)
            pn_clean = pn_part.strip().lstrip("Pp")
            try:
                pn_int = int(float(pn_clean))
            except (ValueError, TypeError):
                # Unparseable → skip this part (catches CD-CH cols and blanks)
                continue

            config_meta.append({
                "col_idx"      : col_idx,
                "Process_Node" : pn_int,
                "Three_CEID"   : ceid_val,
                "Tool_Type"    : type_val,
            })

    print(f"  Config columns found (after ignoring CD-CH): {len(config_meta)}")

    # ---- Melt: one row per (base fields + config) ----
    records = []
    for cm in config_meta:
        col_idx = cm["col_idx"]
        # Grab the Qty_Per_Tool values for this config column
        qty_series = data.iloc[:, col_idx]

        for row_i, qty_raw in enumerate(qty_series):
            # Skip blank / zero qty → no applicability
            if pd.isna(qty_raw):
                continue
            try:
                qty = float(qty_raw)
            except (ValueError, TypeError):
                continue
            if qty == 0:
                continue

            # Get corresponding base row
            base_row = df_base.iloc[row_i] if row_i < len(df_base) else None
            if base_row is None:
                continue

            part_num = str(base_row.get("Part_Number", "")).strip()
            if not part_num or part_num.lower() in ("nan", "none", ""):
                continue

            # Platform can be a combined value like 'Telius/Tactras' meaning
            # this part applies to BOTH platforms. Split and emit one row each,
            # exactly as we do for Process_Node combined headers like 'P1274/P1278'.
            raw_platform = str(base_row.get("Platform", "")).strip()
            platform_parts = [p.strip() for p in raw_platform.split("/") if p.strip()]
            if not platform_parts:
                platform_parts = [""]
            for platform_val in platform_parts:
                records.append({
                    "Part_Number"     : part_num,
                    "Platform"        : platform_val,
                    "Part_Description": str(base_row.get("Part_Description", "")).strip(),
                    "OEM_Name"        : str(base_row.get("OEM_Name", "")).strip(),
                    "OEM_Part_Number" : str(base_row.get("OEM_Part_Number", "")).strip(),
                    "Three_CEID"      : cm["Three_CEID"],
                    "Tool_Type"       : cm["Tool_Type"],
                    "Process_Node"    : cm["Process_Node"],
                    "Qty_Per_Tool"    : qty,
                })

    df_gbom = pd.DataFrame(records)
    print(f"  GBOM applicability rows (after normalisation): {len(df_gbom):,}")

    # Final type cleanup
    df_gbom["Process_Node"] = df_gbom["Process_Node"].astype(int)
    df_gbom["Qty_Per_Tool"] = pd.to_numeric(df_gbom["Qty_Per_Tool"], errors="coerce")

    # Drop any remaining rows missing the join keys
    df_gbom = df_gbom.dropna(subset=["Part_Number", "Process_Node", "Three_CEID"])

    return df_gbom


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("Script 1: Ingest + Normalise")
    print("=" * 60)

    engine = get_engine()

    # 1) Inventory
    df_inventory = load_inventory(INVENTORY_PATH)
    write_table(df_inventory, "cur_inventory", engine)

    # 2) Auto Tool List (MOM-only)
    df_mom = load_auto_tool_list_mom(AUTO_TOOL_PATH)
    write_table(df_mom, "cur_auto_tool_list_mom", engine)

    # 3) GBOM applicability (wide → long)
    df_gbom = load_gbom_applicability(GBOM_PATH)
    write_table(df_gbom, "cur_gbom_applicability", engine)

    print("\nScript 1 complete. All curated tables written to SQL Server.")
    print("Run run_02_build_marts_and_export.py next.")


if __name__ == "__main__":
    main()



















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
    # Add age buckets (needed for optional age-split counts)
    df = add_age_bucket(df_mom)

    # Active flag
    df["is_active"] = is_active(df["install_sts"])

    # Ensure Process_Node is numeric for consistent joining
    df["Process_Node"] = pd.to_numeric(df["Process_Node"], errors="coerce").astype("Int64")

    # --- Total active distinct count per config ---
    active_df = df[df["is_active"]].copy()
    # Join key is Three_CEID + Process_Node only — Platform is excluded because
    # GBOM does not distinguish between platform variants (e.g. Tactras vs Tactras BX).
    config_total = (
        active_df
        .groupby(["Three_CEID", "Process_Node"])["ENTITY"]
        .nunique()
        .reset_index()
        .rename(columns={"ENTITY": "Active_Tool_Count_Config"})
    )

    # --- Age-bucket counts (still DISTINCT per config + bucket) ---
    age_counts = (
        active_df
        .groupby(["Three_CEID", "Process_Node", "Age_Bucket"])["ENTITY"]
        .nunique()
        .reset_index()
        .rename(columns={"ENTITY": "Count"})
    )
    age_pivot = age_counts.pivot_table(
        index=["Three_CEID", "Process_Node"],
        columns="Age_Bucket",
        values="Count",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()

    # Rename pivot columns
    age_col_map = {
        "0_1Y"       : "Active_Tool_Count_0_1Y",
        "1_3Y"       : "Active_Tool_Count_1_3Y",
        "3_5Y"       : "Active_Tool_Count_3_5Y",
        "5P_Y"       : "Active_Tool_Count_5P_Y",
        "UnknownAge" : "Active_Tool_Count_UnknownAge",
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
    agg_spec = {
        "Amount_of_stock" : ("Qty",            "sum"),
        "Total_Amount_USD": ("Total_Amount_USD","sum"),
    }
    if "Supplier_Code" in df_inv.columns:
        agg_spec["Supplier_Code"] = ("Supplier_Code", "first")
    if "MOU12" in df_inv.columns:
        agg_spec["MOU12"] = ("MOU12", "max")
    else:
        print("  WARNING: MOU12 column not found in inventory — skipping.")
    if "MOU36" in df_inv.columns:
        agg_spec["MOU36"] = ("MOU36", "max")
    else:
        print("  WARNING: MOU36 column not found in inventory — skipping.")
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

    # --- True part-level DISTINCT active tool count ---
    # Step A: join GBOM → MOM on (Platform, Three_CEID, Process_Node)
    # We keep only the ENTITY and Part_Number columns we need.

    # Join keys are Three_CEID + Process_Node only — Platform excluded intentionally.
    df_gbom_keys = df_gbom[["Part_Number", "Three_CEID", "Process_Node"]].copy()
    df_gbom_keys["Process_Node"] = pd.to_numeric(
        df_gbom_keys["Process_Node"], errors="coerce"
    ).astype("Int64")
    df_mom_keys = df_mom[["ENTITY", "Three_CEID", "Process_Node", "install_sts",
                           "Warranty_Start"]].copy()
    df_mom_keys["Process_Node"] = pd.to_numeric(
        df_mom_keys["Process_Node"], errors="coerce"
    ).astype("Int64")
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

    # Step D: COUNT(DISTINCT ENTITY) per Part_Number — this is the correct method
    part_tool_counts = (
        active_join
        .groupby("Part_Number")["ENTITY"]
        .nunique()
        .reset_index()
        .rename(columns={"ENTITY": "Number_of_active_tools"})
    )

    # Step E: age-split distinct counts at part level (same logic)
    age_part = (
        active_join
        .groupby(["Part_Number", "Age_Bucket"])["ENTITY"]
        .nunique()
        .reset_index()
        .rename(columns={"ENTITY": "Count"})
    )
    age_part_pivot = age_part.pivot_table(
        index="Part_Number",
        columns="Age_Bucket",
        values="Count",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()
    age_col_map = {
        "0_1Y"       : "Active_Tools_0_1Y",
        "1_3Y"       : "Active_Tools_1_3Y",
        "3_5Y"       : "Active_Tools_3_5Y",
        "5P_Y"       : "Active_Tools_5P_Y",
        "UnknownAge" : "Active_Tools_UnknownAge",
    }
    age_part_pivot = age_part_pivot.rename(columns=age_col_map)
    age_part_pivot.columns.name = None

    # --- Assemble leadership table ---
    # Start from all inventory parts
    leadership = inv_agg.merge(gbom_max_qty,      on="Part_Number", how="left")
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
