def delete_dates_from_table(config: Dict, table_name: str, dates: list, date_column: str):
    """Delete specific dates from a table before re-inserting."""
    if not dates:
        return
    
    from utils.database_engine import get_database_connection
    
    # Convert dates to strings for SQL
    date_strings = [str(d) for d in dates]
    date_list = "','".join(date_strings)
    delete_query = f"DELETE FROM dbo.{table_name} WHERE {date_column} IN ('{date_list}')"
    
    conn = get_database_connection(config)
    cursor = conn.cursor()
    cursor.execute(delete_query)
    conn.commit()
    cursor.close()
    conn.close()
    
    logger.info(f"Deleted {len(dates)} dates from {table_name}")
