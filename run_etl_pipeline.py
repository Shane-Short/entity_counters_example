"""
Entity States & Counters ETL Pipeline
======================================
Main orchestration script for the unified EntityStates/Counters ingestion pipeline.

Usage:
    python run_etl_pipeline.py --layer all --mode full
    python run_etl_pipeline.py --layer bronze --mode incremental
    python run_etl_pipeline.py --layer silver --mode full
    python run_etl_pipeline.py --layer gold --mode full

Arguments:
    --layer: bronze, silver, gold, or all
    --mode: full or incremental
"""

import argparse
import yaml
import logging
from pathlib import Path
from datetime import datetime
import sys

# Import Bronze layer modules
from etl.bronze.entity_states_ingestion import run_entity_states_ingestion
from etl.bronze.counters_ingestion import run_counters_ingestion

# Import Silver layer modules
from etl.silver.enrichment import run_silver_enrichment

# Import Gold layer modules
from etl.gold.aggregations import create_gold_facts

# Import utilities
from utils.logger import setup_logger, create_run_log_file
from utils.database_engine import load_to_sqlserver

# Setup logging
log_file = create_run_log_file('logs')
logger = setup_logger(__name__, level='INFO', log_file=log_file)


class EntityCountersETL:
    """
    Main ETL pipeline orchestrator.
    """
    
    def __init__(self, config_path: str = 'config/config.yaml'):
        """
        Initialize ETL pipeline.
        
        Parameters
        ----------
        config_path : str
            Path to configuration file
        """
        self.config_path = config_path
        self.config = self.load_config()
        
        logger.info("=" * 80)
        logger.info("Entity States & Counters ETL Pipeline")
        logger.info("=" * 80)
        logger.info(f"Config loaded from: {config_path}")
    
    def load_config(self) -> dict:
        """
        Load configuration from YAML file.
        
        Returns
        -------
        dict
            Configuration dictionary
        """
        config_file = Path(self.config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_file}")
        
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        return config
    
    def run_bronze_layer(self, mode: str = 'full'):
        """
        Run Bronze layer ingestion.
        
        Parameters
        ----------
        mode : str
            'full' or 'incremental'
        """
        logger.info("")
        logger.info("=" * 80)
        logger.info(f"BRONZE LAYER - {mode.upper()} MODE")
        logger.info("=" * 80)
        
        # Ingest EntityStates
        logger.info("Step 1/2: EntityStates Ingestion")
        entity_states_df = run_entity_states_ingestion(self.config, mode=mode)
        
        if not entity_states_df.empty:
            rows_loaded = load_to_sqlserver(
                entity_states_df,
                self.config,
                'ENTITY_STATES_SQLSERVER_OUTPUT',
                if_exists='append'
            )
            logger.info(f"EntityStates: {rows_loaded} rows loaded to SQL Server")
        else:
            logger.warning("EntityStates: No data to load")
        
        # Ingest Counters
        logger.info("Step 2/2: Counters Ingestion")
        counters_df = run_counters_ingestion(self.config, mode=mode)
        
        if not counters_df.empty:
            rows_loaded = load_to_sqlserver(
                counters_df,
                self.config,
                'COUNTERS_SQLSERVER_OUTPUT',
                if_exists='append'
            )
            logger.info(f"Counters: {rows_loaded} rows loaded to SQL Server")
        else:
            logger.warning("Counters: No data to load")
        
        logger.info("BRONZE LAYER COMPLETE")
        
        return entity_states_df, counters_df
    
    def run_silver_layer(self, entity_states_df=None, counters_df=None, mode: str = 'full'):
        """
        Run Silver layer enrichment.
        
        Parameters
        ----------
        entity_states_df : pd.DataFrame, optional
            EntityStates data (if not provided, loads from Bronze)
        counters_df : pd.DataFrame, optional
            Counters data (if not provided, loads from Bronze)
        mode : str
            'full' or 'incremental'
        """
        logger.info("")
        logger.info("=" * 80)
        logger.info(f"SILVER LAYER - {mode.upper()} MODE")
        logger.info("=" * 80)
        
        # If data not provided, load from Bronze tables
        if entity_states_df is None or counters_df is None:
            logger.info("Loading data from Bronze tables...")
            # TODO: Implement Bronze table loading
            # For now, re-run Bronze ingestion
            entity_states_df, counters_df = self.run_bronze_layer(mode)
        
        # Run enrichment
        state_hours_df, production_df, replacements_df = run_silver_enrichment(
            self.config,
            entity_states_df,
            counters_df
        )
        
        # Load to SQL Server
        logger.info("Loading Silver tables to SQL Server...")
        
        # State hours
        if not state_hours_df.empty:
            # Note: Will need to add STATE_HOURS_SQLSERVER_OUTPUT to config
            logger.info(f"State hours: {len(state_hours_df)} rows calculated")
        
        # Wafer production
        if not production_df.empty:
            # Note: Will need to add WAFER_PRODUCTION_SQLSERVER_OUTPUT to config
            logger.info(f"Wafer production: {len(production_df)} rows calculated")
        
        # Part replacements
        if not replacements_df.empty:
            # Note: Will need to add PART_REPLACEMENTS_SQLSERVER_OUTPUT to config
            logger.info(f"Part replacements: {len(replacements_df)} replacement events tracked")
        
        logger.info("SILVER LAYER COMPLETE")
        
        return state_hours_df, production_df, replacements_df
    
    def run_gold_layer(self, production_df=None, state_hours_df=None, mode: str = 'full'):
        """
        Run Gold layer aggregations.
        
        Parameters
        ----------
        production_df : pd.DataFrame, optional
            Wafer production data (if not provided, loads from Silver)
        state_hours_df : pd.DataFrame, optional
            State hours data (if not provided, loads from Silver)
        mode : str
            'full' or 'incremental'
        """
        logger.info("")
        logger.info("=" * 80)
        logger.info(f"GOLD LAYER - {mode.upper()} MODE")
        logger.info("=" * 80)
        
        # If data not provided, load from Silver tables
        if production_df is None or state_hours_df is None:
            logger.info("Loading data from Silver tables...")
            # TODO: Implement Silver table loading
            # For now, re-run Silver enrichment
            entity_states_df, counters_df = self.run_bronze_layer(mode)
            state_hours_df, production_df, _ = self.run_silver_layer(entity_states_df, counters_df, mode)
        
        # Create Gold facts
        daily_prod, weekly_prod, daily_state, weekly_state = create_gold_facts(
            self.config,
            production_df,
            state_hours_df
        )
        
        # Load to SQL Server
        logger.info("Loading Gold tables to SQL Server...")
        
        if not daily_prod.empty:
            logger.info(f"Daily production: {len(daily_prod)} rows aggregated")
        
        if not weekly_prod.empty:
            logger.info(f"Weekly production: {len(weekly_prod)} rows aggregated")
        
        if not daily_state.empty:
            logger.info(f"Daily state hours: {len(daily_state)} rows aggregated")
        
        if not weekly_state.empty:
            logger.info(f"Weekly state hours: {len(weekly_state)} rows aggregated")
        
        logger.info("GOLD LAYER COMPLETE")
        
        return daily_prod, weekly_prod, daily_state, weekly_state
    
    def run_pipeline(self, layer: str = 'all', mode: str = 'full'):
        """
        Run complete ETL pipeline.
        
        Parameters
        ----------
        layer : str
            Which layer to run: 'bronze', 'silver', 'gold', or 'all'
        mode : str
            'full' or 'incremental'
        """
        start_time = datetime.now()
        logger.info(f"Pipeline started at {start_time}")
        logger.info(f"Running layer(s): {layer}")
        logger.info(f"Mode: {mode}")
        
        try:
            if layer == 'all':
                # Run all layers in sequence
                entity_states_df, counters_df = self.run_bronze_layer(mode)
                state_hours_df, production_df, replacements_df = self.run_silver_layer(
                    entity_states_df, counters_df, mode
                )
                daily_prod, weekly_prod, daily_state, weekly_state = self.run_gold_layer(
                    production_df, state_hours_df, mode
                )
            
            elif layer == 'bronze':
                self.run_bronze_layer(mode)
            
            elif layer == 'silver':
                self.run_silver_layer(mode=mode)
            
            elif layer == 'gold':
                self.run_gold_layer(mode=mode)
            
            else:
                raise ValueError(f"Invalid layer: {layer}. Must be 'bronze', 'silver', 'gold', or 'all'")
            
            # Success
            end_time = datetime.now()
            duration = end_time - start_time
            
            logger.info("")
            logger.info("=" * 80)
            logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
            logger.info(f"Started: {start_time}")
            logger.info(f"Finished: {end_time}")
            logger.info(f"Duration: {duration}")
            logger.info(f"Log file: {log_file}")
            logger.info("=" * 80)
            
            return True
        
        except Exception as e:
            logger.error("")
            logger.error("=" * 80)
            logger.error("PIPELINE FAILED")
            logger.error("=" * 80)
            logger.error(f"Error: {e}")
            logger.error("=" * 80)
            raise


def main():
    """
    Main entry point for command-line execution.
    """
    parser = argparse.ArgumentParser(
        description='Entity States & Counters ETL Pipeline'
    )
    
    parser.add_argument(
        '--layer',
        type=str,
        default='all',
        choices=['bronze', 'silver', 'gold', 'all'],
        help='Which layer to run (default: all)'
    )
    
    parser.add_argument(
        '--mode',
        type=str,
        default='full',
        choices=['full', 'incremental'],
        help='Full or incremental refresh (default: full)'
    )
    
    parser.add_argument(
        '--config',
        type=str,
        default='config/config.yaml',
        help='Path to config file (default: config/config.yaml)'
    )
    
    args = parser.parse_args()
    
    # Run pipeline
    etl = EntityCountersETL(config_path=args.config)
    success = etl.run_pipeline(layer=args.layer, mode=args.mode)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
