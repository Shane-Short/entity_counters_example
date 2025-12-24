-- ============================================================================
-- Master Database Setup Script
-- ============================================================================
-- Entity States & Counters Pipeline
-- Database: Parts_Counter_Production
-- 
-- Purpose: Creates all tables for Bronze, Silver, and Gold layers
-- 
-- Usage:
--   Execute this script in SQL Server Management Studio
--   OR
--   Run individual layer scripts in order:
--     1. create_bronze_tables.sql
--     2. create_silver_tables.sql
--     3. create_gold_tables.sql
-- ============================================================================

USE Parts_Counter_Production;
GO

PRINT '========================================';
PRINT 'Entity & Counters Pipeline Setup';
PRINT 'Starting database initialization...';
PRINT '========================================';
PRINT '';
GO

-- ============================================================================
-- BRONZE LAYER - Raw Data Tables
-- ============================================================================

PRINT '========================================';
PRINT 'Creating BRONZE layer tables...';
PRINT '========================================';
PRINT '';
GO

-- entity_states_raw
DROP TABLE IF EXISTS dbo.entity_states_raw;
GO

CREATE TABLE dbo.entity_states_raw (
    entity_state_raw_id INT IDENTITY(1,1) PRIMARY KEY,
    FAB VARCHAR(50) NOT NULL,
    WW VARCHAR(20),
    DAY_SHIFT VARCHAR(100) NOT NULL,
    ENTITY_STATE VARCHAR(100) NOT NULL,
    ENTITY VARCHAR(255) NOT NULL,
    HOURS_IN_STATE DECIMAL(10,2),
    Total_Hours DECIMAL(10,2),
    [% in State] DECIMAL(10,4),
    FAB_ENTITY VARCHAR(300) NOT NULL,
    source_file VARCHAR(500),
    load_ww VARCHAR(20),
    load_ts DATETIME2(7),
    load_date DATE,
    INDEX IX_entity_states_raw_FAB_ENTITY (FAB_ENTITY),
    INDEX IX_entity_states_raw_ENTITY (ENTITY),
    INDEX IX_entity_states_raw_DAY_SHIFT (DAY_SHIFT),
    INDEX IX_entity_states_raw_load_date (load_date)
);
GO

PRINT 'Created: dbo.entity_states_raw';
GO

-- counters_raw
DROP TABLE IF EXISTS dbo.counters_raw;
GO

CREATE TABLE dbo.counters_raw (
    counters_raw_id INT IDENTITY(1,1) PRIMARY KEY,
    ENTITY VARCHAR(255) NOT NULL,
    FAB VARCHAR(50) NOT NULL,
    FAB_ENTITY VARCHAR(300) NOT NULL,
    FocusRingCounter DECIMAL(18,2),
    APCCounter DECIMAL(18,2),
    ESCCounter DECIMAL(18,2),
    PMACounter DECIMAL(18,2),
    PMBCounter DECIMAL(18,2),
    PMCCounter DECIMAL(18,2),
    source_file VARCHAR(500),
    load_ww VARCHAR(20),
    load_ts DATETIME2(7),
    counter_date DATE NOT NULL,
    file_modified_ts DATETIME2(7),
    INDEX IX_counters_raw_FAB_ENTITY (FAB_ENTITY),
    INDEX IX_counters_raw_ENTITY (ENTITY),
    INDEX IX_counters_raw_counter_date (counter_date),
    INDEX IX_counters_raw_FAB_ENTITY_date (FAB_ENTITY, counter_date)
);
GO

PRINT 'Created: dbo.counters_raw';
PRINT 'NOTE: counters_raw has sample columns - may need dynamic column creation';
PRINT '';
GO

-- ============================================================================
-- SILVER LAYER - Enriched Data Tables
-- ============================================================================

PRINT '========================================';
PRINT 'Creating SILVER layer tables...';
PRINT '========================================';
PRINT '';
GO

-- state_hours
DROP TABLE IF EXISTS dbo.state_hours;
GO

CREATE TABLE dbo.state_hours (
    state_hours_id INT IDENTITY(1,1) PRIMARY KEY,
    ENTITY VARCHAR(255) NOT NULL,
    FAB VARCHAR(50) NOT NULL,
    FAB_ENTITY VARCHAR(300) NOT NULL,
    state_date DATE NOT NULL,
    running_hours DECIMAL(10,2) DEFAULT 0,
    idle_hours DECIMAL(10,2) DEFAULT 0,
    down_hours DECIMAL(10,2) DEFAULT 0,
    bagged_hours DECIMAL(10,2) DEFAULT 0,
    total_hours DECIMAL(10,2) DEFAULT 0,
    is_bagged BIT DEFAULT 0,
    calculation_timestamp DATETIME2(7),
    INDEX IX_state_hours_ENTITY_date (ENTITY, state_date),
    INDEX IX_state_hours_FAB_ENTITY (FAB_ENTITY),
    INDEX IX_state_hours_state_date (state_date),
    CONSTRAINT UQ_state_hours_ENTITY_date UNIQUE (ENTITY, state_date)
);
GO

PRINT 'Created: dbo.state_hours';
GO

-- wafer_production
DROP TABLE IF EXISTS dbo.wafer_production;
GO

CREATE TABLE dbo.wafer_production (
    wafer_production_id INT IDENTITY(1,1) PRIMARY KEY,
    ENTITY VARCHAR(255) NOT NULL,
    counter_date DATE NOT NULL,
    counter_column_used VARCHAR(255),
    counter_keyword_used VARCHAR(100),
    counter_current_value DECIMAL(18,2),
    counter_previous_value DECIMAL(18,2),
    counter_change DECIMAL(18,2),
    part_replacement_detected BIT DEFAULT 0,
    wafers_produced DECIMAL(18,2),
    running_hours DECIMAL(10,2),
    wafers_per_hour DECIMAL(18,4),
    calculation_notes VARCHAR(MAX),
    calculation_timestamp DATETIME2(7),
    INDEX IX_wafer_production_ENTITY_date (ENTITY, counter_date),
    INDEX IX_wafer_production_counter_date (counter_date),
    INDEX IX_wafer_production_replacements (part_replacement_detected),
    CONSTRAINT UQ_wafer_production_ENTITY_date UNIQUE (ENTITY, counter_date)
);
GO

PRINT 'Created: dbo.wafer_production';
GO

-- part_replacements
DROP TABLE IF EXISTS dbo.part_replacements;
GO

CREATE TABLE dbo.part_replacements (
    part_replacement_id INT IDENTITY(1,1) PRIMARY KEY,
    ENTITY VARCHAR(255) NOT NULL,
    replacement_date DATE NOT NULL,
    part_counter_name VARCHAR(255) NOT NULL,
    last_value_before_replacement DECIMAL(18,2),
    first_value_after_replacement DECIMAL(18,2),
    value_drop DECIMAL(18,2),
    part_wafers_at_replacement DECIMAL(18,2),
    notes VARCHAR(MAX),
    replacement_detected_ts DATETIME2(7),
    INDEX IX_part_replacements_ENTITY (ENTITY),
    INDEX IX_part_replacements_date (replacement_date),
    INDEX IX_part_replacements_part (part_counter_name),
    INDEX IX_part_replacements_ENTITY_date (ENTITY, replacement_date),
    CONSTRAINT UQ_part_replacements_ENTITY_date_part UNIQUE (ENTITY, replacement_date, part_counter_name)
);
GO

PRINT 'Created: dbo.part_replacements';
PRINT '';
GO

-- ============================================================================
-- GOLD LAYER - KPI Fact Tables
-- ============================================================================

PRINT '========================================';
PRINT 'Creating GOLD layer tables...';
PRINT '========================================';
PRINT '';
GO

-- fact_daily_production
DROP TABLE IF EXISTS dbo.fact_daily_production;
GO

CREATE TABLE dbo.fact_daily_production (
    daily_production_id INT IDENTITY(1,1) PRIMARY KEY,
    ENTITY VARCHAR(255) NOT NULL,
    FAB VARCHAR(50) NOT NULL,
    FAB_ENTITY VARCHAR(300) NOT NULL,
    production_date DATE NOT NULL,
    wafers_produced DECIMAL(18,2),
    wafers_per_hour DECIMAL(18,4),
    running_hours DECIMAL(10,2),
    idle_hours DECIMAL(10,2),
    down_hours DECIMAL(10,2),
    bagged_hours DECIMAL(10,2),
    total_hours DECIMAL(10,2),
    is_bagged BIT DEFAULT 0,
    part_replacement_detected BIT DEFAULT 0,
    counter_column_used VARCHAR(255),
    counter_keyword_used VARCHAR(100),
    calculation_timestamp DATETIME2(7),
    INDEX IX_fact_daily_production_ENTITY_date (ENTITY, production_date),
    INDEX IX_fact_daily_production_FAB_ENTITY (FAB_ENTITY),
    INDEX IX_fact_daily_production_date (production_date),
    INDEX IX_fact_daily_production_FAB (FAB),
    CONSTRAINT UQ_fact_daily_production_ENTITY_date UNIQUE (ENTITY, production_date)
);
GO

PRINT 'Created: dbo.fact_daily_production';
GO

-- fact_weekly_production
DROP TABLE IF EXISTS dbo.fact_weekly_production;
GO

CREATE TABLE dbo.fact_weekly_production (
    weekly_production_id INT IDENTITY(1,1) PRIMARY KEY,
    ENTITY VARCHAR(255) NOT NULL,
    FAB VARCHAR(50) NOT NULL,
    FAB_ENTITY VARCHAR(300) NOT NULL,
    YEARWW VARCHAR(20) NOT NULL,
    total_wafers_produced DECIMAL(18,2),
    total_running_hours DECIMAL(10,2),
    total_idle_hours DECIMAL(10,2),
    total_down_hours DECIMAL(10,2),
    total_bagged_hours DECIMAL(10,2),
    total_hours DECIMAL(10,2),
    avg_wafers_per_hour DECIMAL(18,4),
    part_replacements_count INT DEFAULT 0,
    week_start_date DATE,
    week_end_date DATE,
    days_with_data INT,
    calculation_timestamp DATETIME2(7),
    INDEX IX_fact_weekly_production_ENTITY_WW (ENTITY, YEARWW),
    INDEX IX_fact_weekly_production_FAB_ENTITY (FAB_ENTITY),
    INDEX IX_fact_weekly_production_YEARWW (YEARWW),
    INDEX IX_fact_weekly_production_FAB (FAB),
    CONSTRAINT UQ_fact_weekly_production_ENTITY_WW UNIQUE (ENTITY, YEARWW)
);
GO

PRINT 'Created: dbo.fact_weekly_production';
GO

-- fact_state_hours_daily
DROP TABLE IF EXISTS dbo.fact_state_hours_daily;
GO

CREATE TABLE dbo.fact_state_hours_daily (
    daily_state_hours_id INT IDENTITY(1,1) PRIMARY KEY,
    ENTITY VARCHAR(255) NOT NULL,
    FAB VARCHAR(50) NOT NULL,
    FAB_ENTITY VARCHAR(300) NOT NULL,
    state_date DATE NOT NULL,
    running_hours DECIMAL(10,2) DEFAULT 0,
    idle_hours DECIMAL(10,2) DEFAULT 0,
    down_hours DECIMAL(10,2) DEFAULT 0,
    bagged_hours DECIMAL(10,2) DEFAULT 0,
    total_hours DECIMAL(10,2) DEFAULT 0,
    running_pct DECIMAL(10,4),
    idle_pct DECIMAL(10,4),
    down_pct DECIMAL(10,4),
    is_bagged BIT DEFAULT 0,
    calculation_timestamp DATETIME2(7),
    INDEX IX_fact_state_hours_daily_ENTITY_date (ENTITY, state_date),
    INDEX IX_fact_state_hours_daily_FAB_ENTITY (FAB_ENTITY),
    INDEX IX_fact_state_hours_daily_date (state_date),
    INDEX IX_fact_state_hours_daily_FAB (FAB),
    CONSTRAINT UQ_fact_state_hours_daily_ENTITY_date UNIQUE (ENTITY, state_date)
);
GO

PRINT 'Created: dbo.fact_state_hours_daily';
GO

-- fact_state_hours_weekly
DROP TABLE IF EXISTS dbo.fact_state_hours_weekly;
GO

CREATE TABLE dbo.fact_state_hours_weekly (
    weekly_state_hours_id INT IDENTITY(1,1) PRIMARY KEY,
    ENTITY VARCHAR(255) NOT NULL,
    FAB VARCHAR(50) NOT NULL,
    FAB_ENTITY VARCHAR(300) NOT NULL,
    YEARWW VARCHAR(20) NOT NULL,
    total_running_hours DECIMAL(10,2) DEFAULT 0,
    total_idle_hours DECIMAL(10,2) DEFAULT 0,
    total_down_hours DECIMAL(10,2) DEFAULT 0,
    total_bagged_hours DECIMAL(10,2) DEFAULT 0,
    total_hours DECIMAL(10,2) DEFAULT 0,
    running_pct DECIMAL(10,4),
    idle_pct DECIMAL(10,4),
    down_pct DECIMAL(10,4),
    was_bagged_any_day BIT DEFAULT 0,
    week_start_date DATE,
    week_end_date DATE,
    days_with_data INT,
    calculation_timestamp DATETIME2(7),
    INDEX IX_fact_state_hours_weekly_ENTITY_WW (ENTITY, YEARWW),
    INDEX IX_fact_state_hours_weekly_FAB_ENTITY (FAB_ENTITY),
    INDEX IX_fact_state_hours_weekly_YEARWW (YEARWW),
    INDEX IX_fact_state_hours_weekly_FAB (FAB),
    CONSTRAINT UQ_fact_state_hours_weekly_ENTITY_WW UNIQUE (ENTITY, YEARWW)
);
GO

PRINT 'Created: dbo.fact_state_hours_weekly';
PRINT '';
GO

-- ============================================================================
-- Setup Complete
-- ============================================================================

PRINT '========================================';
PRINT 'Database setup complete!';
PRINT '========================================';
PRINT '';
PRINT 'BRONZE LAYER (2 tables):';
PRINT '  - entity_states_raw';
PRINT '  - counters_raw';
PRINT '';
PRINT 'SILVER LAYER (3 tables):';
PRINT '  - state_hours';
PRINT '  - wafer_production';
PRINT '  - part_replacements';
PRINT '';
PRINT 'GOLD LAYER (4 tables):';
PRINT '  - fact_daily_production';
PRINT '  - fact_weekly_production';
PRINT '  - fact_state_hours_daily';
PRINT '  - fact_state_hours_weekly';
PRINT '';
PRINT 'Total: 9 tables created';
PRINT '';
PRINT 'Next steps:';
PRINT '  1. Run the ETL pipeline to populate Bronze tables';
PRINT '  2. Execute Silver enrichment calculations';
PRINT '  3. Execute Gold aggregations';
PRINT '  4. Connect Power BI to Gold fact tables';
PRINT '========================================';
GO
