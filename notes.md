Current_Unscheduled_State = 
VAR CurrentEntity = SELECTEDVALUE(state_hours_detail[FAB_ENTITY])
VAR LatestDate = CALCULATE(MAX(state_hours_detail[state_date]), ALL(state_hours_detail[state_date]))
VAR CurrentStates = 
    CALCULATETABLE(
        DISTINCT(state_hours_detail[state_name]),
        state_hours_detail[FAB_ENTITY] = CurrentEntity,
        state_hours_detail[state_date] = LatestDate,
        CONTAINSSTRING(state_hours_detail[state_name], "Unsch"),
        state_hours_detail[hours] > 0
    )
RETURN
    IF(
        ISBLANK(CurrentEntity),
        BLANK(),
        CONCATENATEX(CurrentStates, state_hours_detail[state_name], ", ")
    )
