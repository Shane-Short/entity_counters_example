-- ============================================================================
-- Silver Layer - Table Definitions
-- ============================================================================
-- Database: Parts_Counter_Production
-- Schema: dbo
-- Purpose: Enriched tables with calculated metrics and classifications
-- ============================================================================

USE Parts_Counter_Production;
GO

-- ============================================================================
-- state_hours
-- ============================================================================
-- Purpose: Daily state hours by entity (Running, Idle, Down, Bagged)
-- Grain: One row per entity per day
-- Source: Calculated from entity_states_raw
-- ============================================================================

DROP TABLE IF EXISTS dbo.state_hours;
GO

CREATE TABLE dbo.state_hours (
    -- Primary Key
    state_hours_id INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Entity Identifiers
    ENTITY VARCHAR(255) NOT NULL,
    FAB VARCHAR(50) NOT NULL,
    FAB_ENTITY VARCHAR(300) NOT NULL,
    state_date DATE NOT NULL,
    
    -- State Hours (calculated)
    running_hours DECIMAL(10,2) DEFAULT 0,
    idle_hours DECIMAL(10,2) DEFAULT 0,
    down_hours DECIMAL(10,2) DEFAULT 0,
    bagged_hours DECIMAL(10,2) DEFAULT 0,
    total_hours DECIMAL(10,2) DEFAULT 0,
    
    -- Status Flags
    is_bagged BIT DEFAULT 0,
    
    -- Metadata
    calculation_timestamp DATETIME2(7),
    
    -- Indexes
    INDEX IX_state_hours_ENTITY_date (ENTITY, state_date),
    INDEX IX_state_hours_FAB_ENTITY (FAB_ENTITY),
    INDEX IX_state_hours_state_date (state_date),
    
    -- Unique constraint to prevent duplicates
    CONSTRAINT UQ_state_hours_ENTITY_date UNIQUE (ENTITY, state_date)
);
GO

PRINT 'Table created: dbo.state_hours';
GO


-- ============================================================================
-- wafer_production
-- ============================================================================
-- Purpose: Daily wafer production metrics from part counter changes
-- Grain: One row per entity per day
-- Source: Calculated from counters_raw and state_hours
-- ============================================================================

DROP TABLE IF EXISTS dbo.wafer_production;
GO

CREATE TABLE dbo.wafer_production (
    -- Primary Key
    wafer_production_id INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Entity Identifiers
    ENTITY VARCHAR(255) NOT NULL,
    counter_date DATE NOT NULL,
    
    -- Counter Information
    counter_column_used VARCHAR(255),
    counter_keyword_used VARCHAR(100),
    counter_current_value DECIMAL(18,2),
    counter_previous_value DECIMAL(18,2),
    counter_change DECIMAL(18,2),
    
    -- Part Replacement Detection
    part_replacement_detected BIT DEFAULT 0,
    
    -- Production Metrics
    wafers_produced DECIMAL(18,2),
    running_hours DECIMAL(10,2),
    wafers_per_hour DECIMAL(18,4),
    
    -- Calculation Notes
    calculation_notes VARCHAR(MAX),
    
    -- Metadata
    calculation_timestamp DATETIME2(7),
    
    -- Indexes
    INDEX IX_wafer_production_ENTITY_date (ENTITY, counter_date),
    INDEX IX_wafer_production_counter_date (counter_date),
    INDEX IX_wafer_production_replacements (part_replacement_detected),
    
    -- Unique constraint to prevent duplicates
    CONSTRAINT UQ_wafer_production_ENTITY_date UNIQUE (ENTITY, counter_date)
);
GO

PRINT 'Table created: dbo.wafer_production';
GO


-- ============================================================================
-- part_replacements
-- ============================================================================
-- Purpose: Track all detected part replacement events
-- Grain: One row per entity per replacement date per part counter
-- Source: Extracted from wafer_production where part_replacement_detected = TRUE
-- ============================================================================

DROP TABLE IF EXISTS dbo.part_replacements;
GO

CREATE TABLE dbo.part_replacements (
    -- Primary Key
    part_replacement_id INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Entity Identifier
    ENTITY VARCHAR(255) NOT NULL,
    replacement_date DATE NOT NULL,
    
    -- Part Information
    part_counter_name VARCHAR(255) NOT NULL,
    last_value_before_replacement DECIMAL(18,2),
    first_value_after_replacement DECIMAL(18,2),
    value_drop DECIMAL(18,2),
    part_wafers_at_replacement DECIMAL(18,2),
    
    -- Notes
    notes VARCHAR(MAX),
    
    -- Metadata
    replacement_detected_ts DATETIME2(7),
    
    -- Indexes
    INDEX IX_part_replacements_ENTITY (ENTITY),
    INDEX IX_part_replacements_date (replacement_date),
    INDEX IX_part_replacements_part (part_counter_name),
    INDEX IX_part_replacements_ENTITY_date (ENTITY, replacement_date),
    
    -- Unique constraint to prevent duplicates
    CONSTRAINT UQ_part_replacements_ENTITY_date_part UNIQUE (ENTITY, replacement_date, part_counter_name)
);
GO

PRINT 'Table created: dbo.part_replacements';
GO


-- ============================================================================
-- Silver Layer Tables Complete
-- ============================================================================
PRINT '';
PRINT 'Silver layer tables created successfully.';
PRINT 'Tables: state_hours, wafer_production, part_replacements';
GO
