-- ============================================================================
-- Gold Layer - Fact Table Definitions
-- ============================================================================
-- Database: Parts_Counter_Production
-- Schema: dbo
-- Purpose: Aggregated KPI tables for Power BI consumption
-- ============================================================================

USE Parts_Counter_Production;
GO

-- ============================================================================
-- fact_daily_production
-- ============================================================================
-- Purpose: Daily production metrics by entity
-- Grain: One row per entity per day
-- Source: Merged from wafer_production and state_hours
-- ============================================================================

DROP TABLE IF EXISTS dbo.fact_daily_production;
GO

CREATE TABLE dbo.fact_daily_production (
    -- Primary Key
    daily_production_id INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Entity Identifiers
    ENTITY VARCHAR(255) NOT NULL,
    FAB VARCHAR(50) NOT NULL,
    FAB_ENTITY VARCHAR(300) NOT NULL,
    production_date DATE NOT NULL,
    
    -- Production Metrics
    wafers_produced DECIMAL(18,2),
    wafers_per_hour DECIMAL(18,4),
    
    -- State Hours
    running_hours DECIMAL(10,2),
    idle_hours DECIMAL(10,2),
    down_hours DECIMAL(10,2),
    bagged_hours DECIMAL(10,2),
    total_hours DECIMAL(10,2),
    
    -- Status Flags
    is_bagged BIT DEFAULT 0,
    part_replacement_detected BIT DEFAULT 0,
    
    -- Counter Information
    counter_column_used VARCHAR(255),
    counter_keyword_used VARCHAR(100),
    
    -- Metadata
    calculation_timestamp DATETIME2(7),
    
    -- Indexes
    INDEX IX_fact_daily_production_ENTITY_date (ENTITY, production_date),
    INDEX IX_fact_daily_production_FAB_ENTITY (FAB_ENTITY),
    INDEX IX_fact_daily_production_date (production_date),
    INDEX IX_fact_daily_production_FAB (FAB),
    
    -- Unique constraint to prevent duplicates
    CONSTRAINT UQ_fact_daily_production_ENTITY_date UNIQUE (ENTITY, production_date)
);
GO

PRINT 'Table created: dbo.fact_daily_production';
GO


-- ============================================================================
-- fact_weekly_production
-- ============================================================================
-- Purpose: Weekly production metrics by entity
-- Grain: One row per entity per work week
-- Source: Aggregated from fact_daily_production
-- ============================================================================

DROP TABLE IF EXISTS dbo.fact_weekly_production;
GO

CREATE TABLE dbo.fact_weekly_production (
    -- Primary Key
    weekly_production_id INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Entity Identifiers
    ENTITY VARCHAR(255) NOT NULL,
    FAB VARCHAR(50) NOT NULL,
    FAB_ENTITY VARCHAR(300) NOT NULL,
    YEARWW VARCHAR(20) NOT NULL,
    
    -- Production Metrics (Aggregated)
    total_wafers_produced DECIMAL(18,2),
    total_running_hours DECIMAL(10,2),
    total_idle_hours DECIMAL(10,2),
    total_down_hours DECIMAL(10,2),
    total_bagged_hours DECIMAL(10,2),
    total_hours DECIMAL(10,2),
    avg_wafers_per_hour DECIMAL(18,4),
    
    -- Event Counts
    part_replacements_count INT DEFAULT 0,
    
    -- Week Metadata
    week_start_date DATE,
    week_end_date DATE,
    days_with_data INT,
    
    -- Metadata
    calculation_timestamp DATETIME2(7),
    
    -- Indexes
    INDEX IX_fact_weekly_production_ENTITY_WW (ENTITY, YEARWW),
    INDEX IX_fact_weekly_production_FAB_ENTITY (FAB_ENTITY),
    INDEX IX_fact_weekly_production_YEARWW (YEARWW),
    INDEX IX_fact_weekly_production_FAB (FAB),
    
    -- Unique constraint to prevent duplicates
    CONSTRAINT UQ_fact_weekly_production_ENTITY_WW UNIQUE (ENTITY, YEARWW)
);
GO

PRINT 'Table created: dbo.fact_weekly_production';
GO


-- ============================================================================
-- fact_state_hours_daily
-- ============================================================================
-- Purpose: Daily state hours and utilization metrics by entity
-- Grain: One row per entity per day
-- Source: Enhanced from state_hours with calculated percentages
-- ============================================================================

DROP TABLE IF EXISTS dbo.fact_state_hours_daily;
GO

CREATE TABLE dbo.fact_state_hours_daily (
    -- Primary Key
    daily_state_hours_id INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Entity Identifiers
    ENTITY VARCHAR(255) NOT NULL,
    FAB VARCHAR(50) NOT NULL,
    FAB_ENTITY VARCHAR(300) NOT NULL,
    state_date DATE NOT NULL,
    
    -- State Hours
    running_hours DECIMAL(10,2) DEFAULT 0,
    idle_hours DECIMAL(10,2) DEFAULT 0,
    down_hours DECIMAL(10,2) DEFAULT 0,
    bagged_hours DECIMAL(10,2) DEFAULT 0,
    total_hours DECIMAL(10,2) DEFAULT 0,
    
    -- Utilization Percentages
    running_pct DECIMAL(10,4),
    idle_pct DECIMAL(10,4),
    down_pct DECIMAL(10,4),
    
    -- Status Flags
    is_bagged BIT DEFAULT 0,
    
    -- Metadata
    calculation_timestamp DATETIME2(7),
    
    -- Indexes
    INDEX IX_fact_state_hours_daily_ENTITY_date (ENTITY, state_date),
    INDEX IX_fact_state_hours_daily_FAB_ENTITY (FAB_ENTITY),
    INDEX IX_fact_state_hours_daily_date (state_date),
    INDEX IX_fact_state_hours_daily_FAB (FAB),
    
    -- Unique constraint to prevent duplicates
    CONSTRAINT UQ_fact_state_hours_daily_ENTITY_date UNIQUE (ENTITY, state_date)
);
GO

PRINT 'Table created: dbo.fact_state_hours_daily';
GO


-- ============================================================================
-- fact_state_hours_weekly
-- ============================================================================
-- Purpose: Weekly state hours and utilization metrics by entity
-- Grain: One row per entity per work week
-- Source: Aggregated from fact_state_hours_daily
-- ============================================================================

DROP TABLE IF EXISTS dbo.fact_state_hours_weekly;
GO

CREATE TABLE dbo.fact_state_hours_weekly (
    -- Primary Key
    weekly_state_hours_id INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Entity Identifiers
    ENTITY VARCHAR(255) NOT NULL,
    FAB VARCHAR(50) NOT NULL,
    FAB_ENTITY VARCHAR(300) NOT NULL,
    YEARWW VARCHAR(20) NOT NULL,
    
    -- State Hours (Aggregated)
    total_running_hours DECIMAL(10,2) DEFAULT 0,
    total_idle_hours DECIMAL(10,2) DEFAULT 0,
    total_down_hours DECIMAL(10,2) DEFAULT 0,
    total_bagged_hours DECIMAL(10,2) DEFAULT 0,
    total_hours DECIMAL(10,2) DEFAULT 0,
    
    -- Utilization Percentages (Calculated from totals)
    running_pct DECIMAL(10,4),
    idle_pct DECIMAL(10,4),
    down_pct DECIMAL(10,4),
    
    -- Status Flags
    was_bagged_any_day BIT DEFAULT 0,
    
    -- Week Metadata
    week_start_date DATE,
    week_end_date DATE,
    days_with_data INT,
    
    -- Metadata
    calculation_timestamp DATETIME2(7),
    
    -- Indexes
    INDEX IX_fact_state_hours_weekly_ENTITY_WW (ENTITY, YEARWW),
    INDEX IX_fact_state_hours_weekly_FAB_ENTITY (FAB_ENTITY),
    INDEX IX_fact_state_hours_weekly_YEARWW (YEARWW),
    INDEX IX_fact_state_hours_weekly_FAB (FAB),
    
    -- Unique constraint to prevent duplicates
    CONSTRAINT UQ_fact_state_hours_weekly_ENTITY_WW UNIQUE (ENTITY, YEARWW)
);
GO

PRINT 'Table created: dbo.fact_state_hours_weekly';
GO


-- ============================================================================
-- Gold Layer Tables Complete
-- ============================================================================
PRINT '';
PRINT 'Gold layer tables created successfully.';
PRINT 'Tables: fact_daily_production, fact_weekly_production, fact_state_hours_daily, fact_state_hours_weekly';
GO


-- ============================================================================
-- Summary of All Tables
-- ============================================================================
PRINT '';
PRINT '========================================';
PRINT 'Entity & Counters Pipeline - All Tables';
PRINT '========================================';
PRINT '';
PRINT 'BRONZE LAYER (Raw Data):';
PRINT '  - entity_states_raw';
PRINT '  - counters_raw';
PRINT '';
PRINT 'SILVER LAYER (Enriched Data):';
PRINT '  - state_hours';
PRINT '  - wafer_production';
PRINT '  - part_replacements';
PRINT '';
PRINT 'GOLD LAYER (KPI Fact Tables):';
PRINT '  - fact_daily_production';
PRINT '  - fact_weekly_production';
PRINT '  - fact_state_hours_daily';
PRINT '  - fact_state_hours_weekly';
PRINT '';
PRINT 'Total Tables Created: 9';
PRINT '========================================';
GO
