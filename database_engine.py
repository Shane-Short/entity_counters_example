"""
Database Engine Utilities
==========================
Handles SQL Server connections and data loading operations.

Features:
- SQL Server connection management
- DataFrame to SQL insertion with error handling
- Batch loading for large datasets
- Transaction support
"""

import pyodbc
import pandas as pd
import logging
from typing import Dict, Optional
import os
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class SQLServerEngine:
    """
    Manages SQL Server database connections and operations.
    """
    
    def __init__(self, config: Dict, table_params_key: str):
        """
        Initialize SQL Server engine.
        
        Parameters
        ----------
        config : dict
            Configuration dictionary
        table_params_key : str
            Key for table parameters in config
        """
        self.config = config
        self.table_params = config['table_parameters'][table_params_key]
        self.sqlserver_config = self.table_params['sqlserver']
        
        # Substitute environment variables
        self.username = os.getenv('SQL_USER', self.sqlserver_config.get('username', ''))
        self.password = os.getenv('SQL_PASS', self.sqlserver_config.get('password', ''))
        
        self.server = self.sqlserver_config['server']
        self.database = self.sqlserver_config['database']
        self.schema = self.sqlserver_config['schema']
        self.table_name = self.sqlserver_config['table_name']
        self.driver = self.sqlserver_config.get('driver', 'ODBC Driver 18 for SQL Server')
        self.trusted_connection = self.sqlserver_config.get('trusted_connection', False)
        
        logger.info(f"SQL Server Engine initialized for {self.database}.{self.schema}.{self.table_name}")
    
    def get_connection_string(self) -> str:
        """
        Build SQL Server connection string.
        
        Returns
        -------
        str
            ODBC connection string
        """
        if self.trusted_connection:
            conn_str = (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"Trusted_Connection=yes;"
                f"TrustServerCertificate=yes;"
            )
        else:
            conn_str = (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
                f"TrustServerCertificate=yes;"
            )
        
        return conn_str
    
    def get_connection(self):
        """
        Get database connection.
        
        Returns
        -------
        pyodbc.Connection
            Database connection object
        """
        conn_str = self.get_connection_string()
        try:
            conn = pyodbc.connect(conn_str)
            logger.info(f"Connected to SQL Server: {self.server}/{self.database}")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to SQL Server: {e}")
            raise
    
    def load_dataframe(self, df: pd.DataFrame, if_exists: str = 'append') -> int:
        """
        Load DataFrame to SQL Server table.
        
        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to load
        if_exists : str
            What to do if table exists: 'append', 'replace', 'fail'
        
        Returns
        -------
        int
            Number of rows inserted
        """
        if df.empty:
            logger.warning("DataFrame is empty, nothing to load")
            return 0
        
        logger.info(f"Loading {len(df)} rows to {self.table_name}")
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Get column names
            columns = df.columns.tolist()
            
            # Build INSERT statement
            placeholders = ','.join(['?' for _ in columns])
            columns_str = ','.join([f"[{col}]" for col in columns])
            
            insert_sql = f"""
                INSERT INTO {self.schema}.{self.table_name} ({columns_str})
                VALUES ({placeholders})
            """
            
            # Convert DataFrame to list of tuples
            data_tuples = [tuple(row) for row in df.values]
            
            # Execute batch insert
            cursor.executemany(insert_sql, data_tuples)
            conn.commit()
            
            rows_inserted = len(data_tuples)
            logger.info(f"Successfully loaded {rows_inserted} rows to {self.table_name}")
            
            return rows_inserted
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error loading data to {self.table_name}: {e}")
            raise
        
        finally:
            cursor.close()
            conn.close()
    
    def truncate_table(self):
        """
        Truncate table (remove all rows).
        """
        logger.info(f"Truncating table {self.schema}.{self.table_name}")
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(f"TRUNCATE TABLE {self.schema}.{self.table_name}")
            conn.commit()
            logger.info(f"Table {self.table_name} truncated successfully")
        
        except Exception as e:
            conn.rollback()
            logger.error(f"Error truncating table {self.table_name}: {e}")
            raise
        
        finally:
            cursor.close()
            conn.close()
    
    def get_row_count(self) -> int:
        """
        Get current row count in table.
        
        Returns
        -------
        int
            Number of rows in table
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {self.schema}.{self.table_name}")
            count = cursor.fetchone()[0]
            return count
        
        finally:
            cursor.close()
            conn.close()


def load_to_sqlserver(df: pd.DataFrame, config: Dict, table_params_key: str, if_exists: str = 'append') -> int:
    """
    Standalone function to load DataFrame to SQL Server.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to load
    config : dict
        Configuration dictionary
    table_params_key : str
        Key for table parameters in config
    if_exists : str
        What to do if table exists
    
    Returns
    -------
    int
        Number of rows inserted
    """
    engine = SQLServerEngine(config, table_params_key)
    return engine.load_dataframe(df, if_exists)
