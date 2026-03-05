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
        "MOU36"                        : "MOU36",
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
