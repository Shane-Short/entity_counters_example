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

    # Canonical column map — add entries here if source names differ
    rename_map = {
        # source name                  : canonical name
        "Region Name"                  : "Region_Name",
        "Supplier Code"                : "Supplier_Code",
        "StorageLocation Description"  : "StorageLocation_Description",
        "Part Number"                  : "Part_Number",
        "Part Description"             : "Part_Description",
        "Qty"                          : "Qty",
        "Total Amount USD"             : "Total_Amount_USD",
        "MOU12"                        : "MOU12",
        "MOU 12"                       : "MOU12",
        "MOU36"                        : "MOU36",
        "MOU 36"                       : "MOU36",
        "ABC LOCAL"                    : "ABC_LOCAL",
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
