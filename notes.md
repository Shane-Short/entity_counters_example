def load_csv_safe(file_path: Path, expected_columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Safely load CSV file with encoding handling.
    """
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
        logger.info(f"Successfully read CSV, shape: {df.shape}")
        return df
    except UnicodeDecodeError:
        logger.warning(f"UTF-8 encoding failed for {file_path}, trying latin-1")
        df = pd.read_csv(file_path, encoding='latin-1')
        logger.info(f"Successfully read CSV with latin-1, shape: {df.shape}")
        return df






# Load CSV
df = load_csv_safe(file_path, expected_columns=None)

# Clean counter columns: replace empty strings with NaN for numeric columns only
# All counter columns end with 'Counter' and should be numeric
counter_cols = [col for col in df.columns if col.endswith('Counter')]
for col in counter_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')






