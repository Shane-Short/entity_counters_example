DECLARE @sql NVARCHAR(MAX) = 'SELECT FAB_ENTITY, counter_date FROM counters_raw WHERE '
DECLARE @conditions NVARCHAR(MAX) = ''

-- Build conditions for all counter columns
SELECT @conditions = @conditions + 
    'AND (' + QUOTENAME(COLUMN_NAME) + ' IS NULL OR ' + QUOTENAME(COLUMN_NAME) + ' BETWEEN -5 AND 5) '
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'counters_raw'
    AND COLUMN_NAME NOT IN (
        'counters_raw_id', 'ENTITY', 'FAB', 'FAB_ENTITY', 
        'source_file', 'load_ww', 'load_ts', 'load_date', 
        'counter_date', 'file_modified_ts'
    )

-- Remove the leading 'AND '
SET @conditions = STUFF(@conditions, 1, 4, '')

-- Add the entity exclusion filter
SET @sql = @sql + @conditions + ' AND ENTITY NOT LIKE ''%_LP%'' AND ENTITY NOT LIKE ''%_VTM%'' AND ENTITY NOT LIKE ''%_LLM%'' ORDER BY FAB_ENTITY, counter_date'

-- Show the query
PRINT @sql

-- Run it
EXEC sp_executesql @sql
