-- Look at F24_TAO216_PM1 around the disconnected date
SELECT 
    FAB_ENTITY,
    counter_date,
    PayForRFCounter,
    TactrasAPCCleanCounter,
    TactrasAPCCounter
FROM counters_raw
WHERE FAB_ENTITY = 'F24_TAO216_PM1'
    AND counter_date BETWEEN '2024-12-26' AND '2024-12-30'
ORDER BY counter_date
