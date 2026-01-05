def get_connection(self):
        """
        Get database connection with retry logic.
        """
        import time
        
        conn_str = self.get_connection_string()
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Connecting to SQL Server (attempt {attempt + 1}/{max_retries})")
                conn = pyodbc.connect(conn_str, timeout=600)
                logger.info("Connection successful")
                return conn
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 10  # 10, 20, 30 seconds
                    logger.warning(f"Connection failed: {e}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Connection failed after {max_retries} attempts")
                    raise


