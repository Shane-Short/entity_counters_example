Current_Unscheduled_Down_Hours = 
VAR LatestDate = MAX(state_hours_detail[state_date])
RETURN
    CALCULATE(
        SUM(state_hours_detail[hours]),
        state_hours_detail[state_date] = LatestDate,
        CONTAINSSTRING(state_hours_detail[state_category], "Unsch"),
        NOT(CONTAINSSTRING(state_hours_detail[ENTITY], "_LP")),
        NOT(CONTAINSSTRING(state_hours_detail[ENTITY], "_VTM")),
        NOT(CONTAINSSTRING(state_hours_detail[ENTITY], "_LLM"))
    )






Tools_Unscheduled_Down_12Plus = 
VAR LatestDate = MAX(state_hours_detail[state_date])
VAR ToolsCurrentlyDown =
    CALCULATETABLE(
        SUMMARIZE(
            state_hours_detail,
            state_hours_detail[FAB_ENTITY],
            "LatestUnschDownHours", 
            CALCULATE(
                SUM(state_hours_detail[hours]), 
                state_hours_detail[state_date] = LatestDate,
                CONTAINSSTRING(state_hours_detail[state_category], "Unsch")
            )
        ),
        state_hours_detail[state_date] = LatestDate,
        NOT(CONTAINSSTRING(state_hours_detail[ENTITY], "_LP")),
        NOT(CONTAINSSTRING(state_hours_detail[ENTITY], "_VTM")),
        NOT(CONTAINSSTRING(state_hours_detail[ENTITY], "_LLM"))
    )
RETURN
    COUNTROWS(
        FILTER(
            ToolsCurrentlyDown,
            [LatestUnschDownHours] >= 12
        )
    )









Tools_Unscheduled_Down_24Plus = 
VAR LatestDate = MAX(state_hours_detail[state_date])
VAR ToolsCurrentlyDown =
    CALCULATETABLE(
        SUMMARIZE(
            state_hours_detail,
            state_hours_detail[FAB_ENTITY],
            "LatestUnschDownHours", 
            CALCULATE(
                SUM(state_hours_detail[hours]), 
                state_hours_detail[state_date] = LatestDate,
                CONTAINSSTRING(state_hours_detail[state_category], "Unsch")
            )
        ),
        state_hours_detail[state_date] = LatestDate,
        NOT(CONTAINSSTRING(state_hours_detail[ENTITY], "_LP")),
        NOT(CONTAINSSTRING(state_hours_detail[ENTITY], "_VTM")),
        NOT(CONTAINSSTRING(state_hours_detail[ENTITY], "_LLM"))
    )
RETURN
    COUNTROWS(
        FILTER(
            ToolsCurrentlyDown,
            [LatestUnschDownHours] >= 24
        )
    )







