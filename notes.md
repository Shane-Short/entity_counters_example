# =============================================================================
# LOCATION MAPPING
# =============================================================================

FAB_TO_LOCATION = {
    "D1D": "Portland",
    "D1X": "Portland", 
    "D1C": "Portland",
    "AFO": "Portland",
    "F11": "Albuquerque",
    "F11X": "Albuquerque",
    "F21": "Albuquerque",
    "F12": "Arizona",
    "F12C": "Arizona",
    "F32": "Arizona",
    "F42": "Arizona",
    "F52": "Arizona",
    "F24": "Ireland",
    "F28": "Israel",
    "MAL": "Malaysia",
}


def map_fab_to_location(fab_code: str) -> str:
    """
    Map FAB code to standardized Location name.
    
    Args:
        fab_code: FAB code string (e.g., 'D1D', 'F11X', 'F32')
        
    Returns:
        Location name (e.g., 'Portland', 'Arizona') or 'Unknown'
    """
    if pd.isna(fab_code) or fab_code is None:
        return "Unknown"
    
    fab_upper = str(fab_code).upper().strip()
    
    # Direct match first
    if fab_upper in FAB_TO_LOCATION:
        return FAB_TO_LOCATION[fab_upper]
    
    # Try prefix matching (e.g., 'D1D-SOMETHING' -> 'D1D')
    for fab_key, location in FAB_TO_LOCATION.items():
        if fab_upper.startswith(fab_key):
            return location
    
    return "Unknown"


def add_location_column(df: pd.DataFrame, fab_column: str = "FACILITY") -> pd.DataFrame:
    """
    Add Location column to DataFrame based on FAB/FACILITY column.
    
    Args:
        df: DataFrame with FAB/FACILITY column
        fab_column: Name of the column containing FAB codes (default: 'FACILITY')
        
    Returns:
        DataFrame with Location column added
    """
    if fab_column not in df.columns:
        df["Location"] = "Unknown"
        return df
    
    df["Location"] = df[fab_column].apply(map_fab_to_location)
    return df







    def _add_metadata(
    self,
    df: pd.DataFrame,
    file_path: Path,
    work_week: str,
) -> pd.DataFrame:
    """
    Add metadata columns to DataFrame.
    """
    df["load_timestamp"] = datetime.now()
    df["source_file"] = str(file_path)
    df["source_ww"] = work_week
    
    # Add Location column from FACILITY
    df = add_location_column(df, fab_column="FACILITY")
    self.logger.info(f"Location distribution: {df['Location'].value_counts().to_dict()}")

    self.logger.debug("Added metadata columns")
    return df







    
