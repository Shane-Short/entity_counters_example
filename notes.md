def normalize_entity_name(entity: str) -> str:
    """
    Normalize entity name by converting _PC suffix to _PM suffix.
    
    PM_Flex uses _PC suffix (e.g., ABC123_PC6) while Entity Counters uses _PM suffix.
    This ensures consistency for Power BI joins.
    
    Args:
        entity: Entity name string (e.g., 'ABC123_PC6')
        
    Returns:
        Normalized entity name (e.g., 'ABC123_PM6')
    """
    if pd.isna(entity) or entity is None:
        return entity
    
    entity_str = str(entity)
    
    # Replace _PC followed by a number with _PM followed by the same number
    # Pattern: _PC followed by digits at end of string
    import re
    normalized = re.sub(r'_PC(\d+)$', r'_PM\1', entity_str)
    
    return normalized


def normalize_entity_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize entity columns by converting _PC suffix to _PM suffix.
    
    Applies normalization to: ENTITY, UNIQUE_ENTITY_ID, PARENT_ENTITY
    
    Args:
        df: DataFrame with entity columns
        
    Returns:
        DataFrame with normalized entity columns
    """
    entity_columns = ['ENTITY', 'UNIQUE_ENTITY_ID', 'PARENT_ENTITY']
    
    for col in entity_columns:
        if col in df.columns:
            df[col] = df[col].apply(normalize_entity_name)
    
    return df






