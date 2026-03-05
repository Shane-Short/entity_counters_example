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

    # Normalise types
    df["Process_Node"] = pd.to_numeric(df["Process_Node"], errors="coerce").astype("Int64")

    if "Warranty_Start" in df.columns:
        df["Warranty_Start"] = pd.to_datetime(df["Warranty_Start"], errors="coerce")

    # Rename location column to a consistent name if needed
    if "Fab" in df.columns and "Location" not in df.columns:
        df = df.rename(columns={"Fab": "Location"})

    return df
