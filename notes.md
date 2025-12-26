def execute_sql_file(conn, sql_file_path):
    """Execute SQL file."""
    print(f"\nExecuting: {sql_file_path}")
    
    with open(sql_file_path, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    # Remove comments and split by GO statements
    import re
    
    # Remove single-line comments
    sql_content = re.sub(r'--.*$', '', sql_content, flags=re.MULTILINE)
    
    # Split by GO (case insensitive, on its own line)
    batches = re.split(r'^\s*GO\s*$', sql_content, flags=re.MULTILINE | re.IGNORECASE)
    
    cursor = conn.cursor()
    executed = 0
    
    for i, batch in enumerate(batches):
        batch = batch.strip()
        
        # Skip empty batches
        if not batch:
            continue
        
        try:
            cursor.execute(batch)
            conn.commit()
            executed += 1
        except Exception as e:
            error_msg = str(e)
            # Only show error if it's not about objects already existing
            if 'already an object' not in error_msg and 'already exists' not in error_msg:
                print(f"    Warning on batch {i+1}: {e}")
    
    cursor.close()
    print(f"    Completed: {sql_file_path} ({executed} batches executed)")
