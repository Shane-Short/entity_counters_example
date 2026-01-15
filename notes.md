DECLARE @sql NVARCHAR(MAX) = 'SELECT COUNT(*) AS disconnected_count FROM counters_raw WHERE '
DECLARE @conditions NVARCHAR(MAX) = ''

SELECT @conditions = @conditions + 
    'AND (' + QUOTENAME(COLUMN_NAME) + ' IS NULL OR ' + QUOTENAME(COLUMN_NAME) + ' BETWEEN -5 AND 5) '
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'counters_raw'
    AND COLUMN_NAME NOT IN (
        'counters_raw_id', 'ENTITY', 'FAB', 'FAB_ENTITY', 
        'source_file', 'load_ww', 'load_ts', 'load_date', 
        'counter_date', 'file_modified_ts'
    )

SET @conditions = STUFF(@conditions, 1, 4, '')
SET @sql = @sql + @conditions

EXEC sp_executesql @sql
