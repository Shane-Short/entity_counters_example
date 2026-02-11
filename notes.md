import re

def clean_part_counter_name(name: str) -> str:
    """
    Clean part_counter_name for display in Power BI.
    """
    if pd.isna(name) or name is None:
        return name
    
    result = str(name)
    
    # Step 1: Remove prefixes
    for prefix in ['Tactras', 'VIGUS', 'tX', 'TELASH', 'GIB']:
        result = result.replace(prefix, '')
    
    # Step 2: Remove 'Counter'
    result = result.replace('Counter', '')
    
    # Step 3: Replace abbreviations BEFORE adding spaces
    result = result.replace('SS', 'SurfaceScan')
    result = result.replace('CLN', 'Clean')
    
    # Step 4: Handle PM patterns - PMX followed by uppercase stays together
    # PMA, PMB, PMXMTC -> keep PM + next letter, then space before remaining uppercase
    result = re.sub(r'(PM[A-Z])([A-Z][a-z])', r'\1 \2', result)
    
    # Step 5: Add space before uppercase followed by lowercase (camelCase split)
    # But keep numbers with previous character
    result = re.sub(r'([a-z])([A-Z])', r'\1 \2', result)
    result = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1 \2', result)
    
    # Step 6: Final cleanup
    result = re.sub(r'\s+', ' ', result)  # Multiple spaces to single
    result = result.strip()
    
    return result
