python -c "
import pandas as pd
from pathlib import Path

file_path = Path(r'\\teas6303\ESL-ProdDataAnalytics\Data\2025WW52\EntityStates.csv')
df = pd.read_csv(file_path)

print('Column Max Lengths:')
for col in df.columns:
    if df[col].dtype == 'object':
        max_len = df[col].astype(str).str.len().max()
        print(f'  {col:30} {max_len:>5} chars')
"




python -c "import pyodbc, yaml; config = yaml.safe_load(open('config/config.yaml')); conn = pyodbc.connect(f\"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={config['table_parameters']['ENTITY_STATES_SQLSERVER_OUTPUT']['sqlserver']['server']};DATABASE={config['table_parameters']['ENTITY_STATES_SQLSERVER_OUTPUT']['sqlserver']['database']};UID={config['table_parameters']['ENTITY_STATES_SQLSERVER_OUTPUT']['sqlserver']['username']};PWD={config['table_parameters']['ENTITY_STATES_SQLSERVER_OUTPUT']['sqlserver']['password']};TrustServerCertificate=yes;\"); cursor = conn.cursor(); cursor.execute(\"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'entity_states_raw' ORDER BY ORDINAL_POSITION\"); print('\\nentity_states_raw columns:'); [print(f'  {row.COLUMN_NAME:30} {row.DATA_TYPE:15} {row.CHARACTER_MAXIMUM_LENGTH if row.CHARACTER_MAXIMUM_LENGTH else \"N/A\"}') for row in cursor.fetchall()]"
