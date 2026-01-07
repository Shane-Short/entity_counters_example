def get_connection_string(self) -> str:
        """
        Build SQL Server connection string using TCP/IP.
        """
        # Parse server to add port if not specified
        if '\\' in self.server:
            # Format: SERVERNAME\INSTANCE
            server_tcp = f"tcp:{self.server}"
        elif ',' not in self.server:
            # Add default port 1433
            server_tcp = f"tcp:{self.server},1433"
        else:
            # Port already specified
            server_tcp = f"tcp:{self.server}"
        
        if self.trusted_connection:
            conn_str = (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={server_tcp};"
                f"DATABASE={self.database};"
                f"Trusted_Connection=yes;"
                f"TrustServerCertificate=yes;"
            )
        else:
            conn_str = (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={server_tcp};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
                f"TrustServerCertificate=yes;"
            )
        
        return conn_str
