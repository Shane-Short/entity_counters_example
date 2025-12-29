def truncate_tables(self, layer: str = 'all'):
        """
        Truncate tables for specified layer(s).
        
        Parameters
        ----------
        layer : str
            Which layer(s) to truncate: 'bronze', 'silver', 'gold', 'all'
        """
        logger.info("")
        logger.info("=" * 80)
        logger.info("TRUNCATING TABLES")
        logger.info("=" * 80)
        
        from utils.database_engine import SQLServerEngine
        
        # Use SQLServerEngine to get proper connection
        engine = SQLServerEngine(self.config, 'ENTITY_STATES_SQLSERVER_OUTPUT')
        conn = engine.get_connection()
        cursor = conn.cursor()
        
        tables_to_truncate = []
        
        # Determine which tables to truncate based on layer
        if layer in ['bronze', 'all']:
            tables_to_truncate.extend([
                'entity_states_raw',
                'counters_raw'
            ])
        
        if layer in ['silver', 'all']:
            tables_to_truncate.extend([
                'state_hours',
                'wafer_production',
                'part_replacements'
            ])
        
        if layer in ['gold', 'all']:
            tables_to_truncate.extend([
                'fact_daily_production',
                'fact_weekly_production',
                'fact_state_hours_daily',
                'fact_state_hours_weekly'
            ])
        
        # Truncate tables
        for table in tables_to_truncate:
            try:
                cursor.execute(f"TRUNCATE TABLE dbo.{table}")
                conn.commit()
                logger.info(f"  Truncated: {table}")
            except Exception as e:
                logger.warning(f"  Could not truncate {table}: {e}")
        
        cursor.close()
        conn.close()
        
        logger.info(f"Truncated {len(tables_to_truncate)} tables")
