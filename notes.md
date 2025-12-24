def execute_sql_file(conn, sql_file_path):
    """Execute SQL file."""
    print(f"\nExecuting: {sql_file_path}")
    
    with open(sql_file_path, 'r') as f:
        sql_content = f.read()
    
    # Split by GO statements (only when GO is on its own line)
    import re
    batches = re.split(r'\n\s*GO\s*\n', sql_content, flags=re.IGNORECASE)
    
    cursor = conn.cursor()
    
    for i, batch in enumerate(batches):
        batch = batch.strip()
        if not batch or batch.startswith('--'):
            continue
        
        try:
            cursor.execute(batch)
            conn.commit()
        except Exception as e:
            # Only print error if it's not about tables already existing
            if 'already an object' not in str(e) and 'already exists' not in str(e):
                print(f"  Warning on batch {i+1}: {e}")





def execute_sql_file(conn, sql_file_path):
    """Execute SQL file."""
    print(f"\nExecuting: {sql_file_path}")
    
    with open(sql_file_path, 'r') as f:
        sql_content = f.read()
    
    # Split by GO statements (only when GO is on its own line)
    import re
    batches = re.split(r'\n\s*GO\s*\n', sql_content, flags=re.IGNORECASE)
    
    cursor = conn.cursor()
    
    for i, batch in enumerate(batches):
        batch = batch.strip()
        if not batch or batch.startswith('--'):
            continue
        
        try:
            cursor.execute(batch)
            conn.commit()
        except Exception as e:
            print(f"  Error on batch {i+1}: {e}")
            raise



