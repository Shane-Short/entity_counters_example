def run_silver_layer(self, entity_states_df: pd.DataFrame, counters_df: pd.DataFrame, mode: str = 'full'):
        """Run Silver layer processing."""
        logger.info("")
        logger.info("=" * 80)
        logger.info(f"SILVER LAYER - {mode.upper()} MODE")
        logger.info("=" * 80)
        
        # TEST MODE: Limit data for faster testing
        if self.config.get('test_mode', {}).get('enabled', False):
            logger.info("TEST MODE: Limiting data to most recent date per entity")
            
            # Keep only most recent date per FAB_ENTITY for entity_states
            entity_states_df = entity_states_df.sort_values('DAY_SHIFT').groupby('FAB_ENTITY').tail(1).reset_index(drop=True)
            logger.info(f"TEST MODE: Filtered entity_states to {len(entity_states_df)} rows (most recent per entity)")
            
            # Keep only most recent date per FAB_ENTITY for counters
            counters_df = counters_df.sort_values('counter_date').groupby('FAB_ENTITY').tail(1).reset_index(drop=True)
            logger.info(f"TEST MODE: Filtered counters to {len(counters_df)} rows (most recent per entity)")
        
        # Continue with existing code...




results = []
        
        # Group by FAB_ENTITY
        total_entities = counters_df['FAB_ENTITY'].nunique()
        entity_count = 0
        
        logger.info(f"Processing {total_entities} unique FAB_ENTITY groups")
        
        for fab_entity, entity_group in counters_df.groupby("FAB_ENTITY"):
            entity_count += 1
            entity = entity_group.iloc[0]["ENTITY"]
            
            # Log progress every 100 entities
            if entity_count % 100 == 0:
                print(f"  Processing entity {entity_count}/{total_entities} ({entity_count/total_entities*100:.1f}%)")
                logger.info(f"Processing entity {entity_count}/{total_entities} ({entity_count/total_entities*100:.1f}%)")






