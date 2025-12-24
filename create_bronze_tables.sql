-- ============================================================================
-- Bronze Layer - Table Definitions
-- ============================================================================
-- Database: Parts_Counter_Production
-- Schema: dbo
-- Purpose: Raw mirror tables for EntityStates.csv and Counters_*.csv files
-- ============================================================================

USE Parts_Counter_Production;
GO

-- ============================================================================
-- entity_states_raw
-- ============================================================================
-- Purpose: Raw mirror of EntityStates.csv files
-- Grain: One row per entity per state per shift per day
-- Source: EntityStates.csv from weekly WW folders
-- ============================================================================

DROP TABLE IF EXISTS dbo.entity_states_raw;
GO

CREATE TABLE dbo.entity_states_raw (
    -- Primary Key
    entity_state_raw_id INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Source Data Columns
    FAB VARCHAR(50) NOT NULL,
    WW VARCHAR(20),
    DAY_SHIFT VARCHAR(100) NOT NULL,
    ENTITY_STATE VARCHAR(100) NOT NULL,
    ENTITY VARCHAR(255) NOT NULL,
    HOURS_IN_STATE DECIMAL(10,2),
    Total_Hours DECIMAL(10,2),
    [% in State] DECIMAL(10,4),
    
    -- Derived Columns
    FAB_ENTITY VARCHAR(300) NOT NULL,
    
    -- Metadata Columns
    source_file VARCHAR(500),
    load_ww VARCHAR(20),
    load_ts DATETIME2(7),
    load_date DATE,
    
    -- Indexes for common queries
    INDEX IX_entity_states_raw_FAB_ENTITY (FAB_ENTITY),
    INDEX IX_entity_states_raw_ENTITY (ENTITY),
    INDEX IX_entity_states_raw_DAY_SHIFT (DAY_SHIFT),
    INDEX IX_entity_states_raw_load_date (load_date)
);
GO

PRINT 'Table created: dbo.entity_states_raw';
GO


-- ============================================================================
-- counters_raw
-- ============================================================================
-- Purpose: Raw mirror of Counters_*.csv files with dynamic part counter columns
-- Grain: One row per entity per counter date
-- Source: Counters_*.csv from weekly WW folders (latest by modified date)
-- Note: Part counter columns vary by tool type - schema will be dynamic
-- ============================================================================

DROP TABLE IF EXISTS dbo.counters_raw;
GO

CREATE TABLE dbo.counters_raw (
    -- Primary Key
    counters_raw_id INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Core Columns
    ENTITY VARCHAR(255) NOT NULL,
    FAB VARCHAR(50) NOT NULL,
    FAB_ENTITY VARCHAR(300) NOT NULL,
    
    -- Part Counter Columns (Dynamic - these are examples, actual columns vary)
    -- Note: In practice, this table should be created with actual columns from first file load
    -- Or use a wide VARCHAR column to store all counters as JSON
    -- For now, including common counters as examples:
    FocusRingCounter DECIMAL(18,2),
    APCCounter DECIMAL(18,2),
    ESCCounter DECIMAL(18,2),
    PMACounter DECIMAL(18,2),
    PMBCounter DECIMAL(18,2),
    PMCCounter DECIMAL(18,2),
    
    -- NOTE: In production, this table should be created dynamically based on
    -- the actual columns in the first Counters file processed, or use a
    -- flexible schema like JSON storage for the counter columns
    
    -- Metadata Columns
    source_file VARCHAR(500),
    load_ww VARCHAR(20),
    load_ts DATETIME2(7),
    counter_date DATE NOT NULL,
    file_modified_ts DATETIME2(7),
    
    -- Indexes for common queries
    INDEX IX_counters_raw_FAB_ENTITY (FAB_ENTITY),
    INDEX IX_counters_raw_ENTITY (ENTITY),
    INDEX IX_counters_raw_counter_date (counter_date),
    INDEX IX_counters_raw_FAB_ENTITY_date (FAB_ENTITY, counter_date)
);
GO

PRINT 'Table created: dbo.counters_raw';
PRINT 'NOTE: counters_raw table has sample part counter columns.';
PRINT 'In production, this should be created dynamically based on actual file columns.';
GO

-- ============================================================================
-- Alternative: JSON-based Counters Table (More Flexible)
-- ============================================================================
-- Uncomment this section if you want to use JSON storage for dynamic columns
/*
DROP TABLE IF EXISTS dbo.counters_raw;
GO

CREATE TABLE dbo.counters_raw (
    counters_raw_id INT IDENTITY(1,1) PRIMARY KEY,
    ENTITY VARCHAR(255) NOT NULL,
    FAB VARCHAR(50) NOT NULL,
    FAB_ENTITY VARCHAR(300) NOT NULL,
    
    -- Store all part counters as JSON
    part_counters NVARCHAR(MAX),
    
    -- Metadata
    source_file VARCHAR(500),
    load_ww VARCHAR(20),
    load_ts DATETIME2(7),
    counter_date DATE NOT NULL,
    file_modified_ts DATETIME2(7),
    
    INDEX IX_counters_raw_FAB_ENTITY (FAB_ENTITY),
    INDEX IX_counters_raw_counter_date (counter_date)
);
GO
*/

-- ============================================================================
-- Bronze Layer Tables Complete
-- ============================================================================
PRINT '';
PRINT 'Bronze layer tables created successfully.';
PRINT 'Tables: entity_states_raw, counters_raw';
GO
