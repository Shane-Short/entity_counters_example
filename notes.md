elif layer == 'silver':
                # Read from Bronze tables
                logger.info("Reading data from Bronze tables")
                from utils.database_engine import SQLServerEngine
                
                # Read entity_states_raw
                engine_es = SQLServerEngine(self.config, 'ENTITY_STATES_SQLSERVER_OUTPUT')
                conn = engine_es.get_connection()
                entity_states_df = pd.read_sql("SELECT * FROM dbo.entity_states_raw", conn)
                conn.close()
                logger.info(f"Read {len(entity_states_df)} rows from entity_states_raw")
                
                # Read counters_raw
                engine_counters = SQLServerEngine(self.config, 'COUNTERS_SQLSERVER_OUTPUT')
                conn = engine_counters.get_connection()
                counters_df = pd.read_sql("SELECT * FROM dbo.counters_raw", conn)
                conn.close()
                logger.info(f"Read {len(counters_df)} rows from counters_raw")
                
                # Run Silver
                self.run_silver_layer(entity_states_df, counters_df, mode=mode)




