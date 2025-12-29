-- ============================================================================
-- State Hours Detail - Individual State Tracking
-- ============================================================================
IF OBJECT_ID('dbo.state_hours_detail', 'U') IS NOT NULL
    DROP TABLE dbo.state_hours_detail;
GO

CREATE TABLE dbo.state_hours_detail (
    state_hours_detail_id INT IDENTITY(1,1) PRIMARY KEY,
    ENTITY VARCHAR(255) NOT NULL,
    FAB VARCHAR(100),
    state_date DATE NOT NULL,
    state_name VARCHAR(200) NOT NULL,
    state_category VARCHAR(50),  -- 'running', 'idle', 'down', 'bagged'
    hours DECIMAL(10,2),
    calculation_timestamp DATETIME2(7) DEFAULT GETDATE(),
    CONSTRAINT UQ_state_hours_detail UNIQUE (ENTITY, state_date, state_name)
);
GO

CREATE INDEX IX_state_hours_detail_entity ON dbo.state_hours_detail(ENTITY);
CREATE INDEX IX_state_hours_detail_date ON dbo.state_hours_detail(state_date);
CREATE INDEX IX_state_hours_detail_state ON dbo.state_hours_detail(state_name);
GO

PRINT 'Created: dbo.state_hours_detail';
GO





def aggregate_by_state(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate hours by entity, date, and individual state.
        Creates a detailed breakdown of all states.
        
        Parameters
        ----------
        df : pd.DataFrame
            EntityStates data
        
        Returns
        -------
        pd.DataFrame
            Aggregated by entity, date, and each unique state
        """
        # Group by ENTITY, state_date, and actual ENTITY_STATE
        state_detail = df.groupby(['ENTITY', 'FAB', 'state_date', 'ENTITY_STATE'])['HOURS_IN_STATE'].sum().reset_index()
        state_detail = state_detail.rename(columns={'HOURS_IN_STATE': 'hours', 'ENTITY_STATE': 'state_name'})
        
        # Add state category for each state
        state_detail['state_category'] = state_detail['state_name'].apply(self.classify_state)
        
        # Add calculation timestamp
        state_detail['calculation_timestamp'] = datetime.now()
        
        logger.info(f"Detailed state tracking: {len(state_detail)} entity-date-state combinations")
        logger.info(f"Unique states found: {state_detail['state_name'].nunique()}")
        
        unique_states = state_detail['state_name'].unique()
        logger.info(f"State breakdown: {', '.join(unique_states[:20])}")  # Show first 20 states
        
        return state_detail


def run_silver_enrichment(config: Dict, entity_states_df: pd.DataFrame, counters_df: pd.DataFrame, mode: str = 'full'):
    """
    Run Silver layer enrichment.
    """
    logger.info("")
    logger.info("=" * 80)
    logger.info(f"SILVER LAYER - {mode.upper()} MODE")
    logger.info("=" * 80)
    
    # STEP 1: Calculate state hours
    logger.info("STEP 1: Calculating state hours")
    state_hours_df = calculate_state_hours(config, entity_states_df)
    
    if not state_hours_df.empty:
        logger.info(f"State hours complete: {len(state_hours_df)} rows")
        
        # Load to SQL Server
        from utils.database_engine import load_to_sqlserver
        rows_loaded = load_to_sqlserver(
            state_hours_df,
            config,
            'STATE_HOURS_SQLSERVER_OUTPUT',
            if_exists='append'
        )
        logger.info(f"State hours: {rows_loaded} rows loaded to SQL Server")
    
    # STEP 1B: Calculate detailed state hours (individual states)
    logger.info("STEP 1B: Calculating detailed state hours")
    calculator = StateHoursCalculator(config)
    state_hours_detail_df = calculator.aggregate_by_state(entity_states_df)
    
    if not state_hours_detail_df.empty:
        logger.info(f"State hours detail complete: {len(state_hours_detail_df)} rows")
        
        # Load to SQL Server
        rows_loaded = load_to_sqlserver(
            state_hours_detail_df,
            config,
            'STATE_HOURS_DETAIL_SQLSERVER_OUTPUT',
            if_exists='append'
        )
        logger.info(f"State hours detail: {rows_loaded} rows loaded to SQL Server")






# Silver layer - State Hours Detail
  STATE_HOURS_DETAIL_SQLSERVER_OUTPUT:
    target: sqlserver
    sqlserver:
      username: ${SQL_USERNAME}
      password: ${SQL_PASSWORD}
      driver: "ODBC Driver 18 for SQL Server"
      server: TEHAUSTELSQL1
      trusted_connection: false
      database: Parts_Counter_Production
      schema: dbo
      table_name: state_hours_detail










