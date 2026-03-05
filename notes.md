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

            records.append({
                "Part_Number"  : part_num,
                "Platform"     : str(base_row.get("Platform", "")).strip(),
                "Part_Description": str(base_row.get("Part_Description", "")).strip(),
                "OEM_Name"     : str(base_row.get("OEM_Name", "")).strip(),
                "OEM_Part_Number": str(base_row.get("OEM_Part_Number", "")).strip(),
                "Three_CEID"   : cm["Three_CEID"],
                "Tool_Type"    : cm["Tool_Type"],
                "Process_Node" : cm["Process_Node"],
                "Qty_Per_Tool" : qty,
            })

    df_gbom = pd.DataFrame(records)
    print(f"  GBOM applicability rows (after normalisation): {len(df_gbom):,}")

    # Final type cleanup
    df_gbom["Process_Node"] = df_gbom["Process_Node"].astype(int)
    df_gbom["Qty_Per_Tool"] = pd.to_numeric(df_gbom["Qty_Per_Tool"], errors="coerce")

    # Drop any remaining rows missing the join keys
    df_gbom = df_gbom.dropna(subset=["Part_Number", "Process_Node", "Three_CEID"])

    return df_gbom
