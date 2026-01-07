-- ============================================================================
-- Silver Layer - State Hours Detail
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
    state_category VARCHAR(50),
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
