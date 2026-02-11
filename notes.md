# =============================================================================
# PART NAME MAPPING - Manual mapping for Power BI display
# =============================================================================

PART_NAME_MAPPING = {
    # Add your mappings here, one per line:
    # "OriginalCounterName": "Clean Display Name",
    "TactrasESCCleanCounter": "ESC Clean",
    "BSHePCV0Counter": "BS He PCV0",
    # ... add the rest here
}


def clean_part_counter_name(name: str) -> str:
    """
    Clean part_counter_name for display in Power BI.
    Uses manual mapping dictionary for predictable results.
    """
    if pd.isna(name) or name is None:
        return name
    
    name_str = str(name)
    
    # Return mapped value if exists, otherwise return original
    return PART_NAME_MAPPING.get(name_str, name_str)
