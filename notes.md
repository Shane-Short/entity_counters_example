def add_location_column(df: pd.DataFrame, fab_column: str = 'FAB') -> pd.DataFrame:
    """
    Adds a Location column to the dataframe based on FAB values.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing FAB column
    fab_column : str
        Name of the FAB column (default: 'FAB')
        
    Returns
    -------
    pd.DataFrame
        DataFrame with Location column added
    """
    import logging
    logger = logging.getLogger(__name__)
    
    df['Location'] = df[fab_column].apply(map_fab_to_location)
    
    # Log the mapping results
    location_counts = df['Location'].value_counts()
    logger.info(f"Location mapping complete: {location_counts.to_dict()}")
    
    unknown_count = (df['Location'] == 'Unknown').sum()
    if unknown_count > 0:
        unknown_fabs = df[df['Location'] == 'Unknown'][fab_column].unique()[:10]
        logger.warning(f"Found {unknown_count} rows with Unknown location. Sample FABs: {unknown_fabs.tolist()}")
    
    return df





