    # Drop unwanted columns by prefix/exact match before anything else.
    # Add more entries here if other junk columns appear in future exports.
    DROP_PREFIXES = ("Active -", "(double check)", "What is the tool model name?")
    cols_to_drop = [c for c in df.columns if str(c).startswith(DROP_PREFIXES)]
    if cols_to_drop:
        print(f"  Dropping {len(cols_to_drop)} unwanted column(s): {cols_to_drop}")
        df = df.drop(columns=cols_to_drop)
