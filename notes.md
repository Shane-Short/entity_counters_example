elif layer == "gold":
                logger.info("Reading data from Silver tables")
                
                from utils.database_engine import get_database_connection
                
                conn = get_database_connection(self.config)
                
                # Read wafer_production
                production_df = pd.read_sql(
                    sql="SELECT * FROM dbo.wafer_production",
                    con=conn,
                )
                logger.info(f"Read {len(production_df)} rows from wafer_production")
                
                # Read state_hours
                state_hours_df = pd.read_sql(
                    sql="SELECT * FROM dbo.state_hours",
                    con=conn,
                )
                logger.info(f"Read {len(state_hours_df)} rows from state_hours")
                
                conn.close()
                
                # Run Gold layer with the data
                self.run_gold_layer(
                    production_df=production_df,
                    state_hours_df=state_hours_df,
                    mode=mode
                )
