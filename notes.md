Continuous_Unscheduled_Down_Hours = 
VAR CurrentEntity = SELECTEDVALUE(state_hours_detail[FAB_ENTITY])
VAR LatestDate = CALCULATE(MAX(state_hours_detail[state_date]), ALL(state_hours_detail[state_date]))

-- Get unscheduled hours per date for this entity
VAR DailyUnschHours = 
    ADDCOLUMNS(
        SUMMARIZE(
            FILTER(
                ALL(state_hours_detail),
                state_hours_detail[FAB_ENTITY] = CurrentEntity
            ),
            state_hours_detail[state_date]
        ),
        "UnschHours",
        CALCULATE(
            SUM(state_hours_detail[hours]),
            CONTAINSSTRING(state_hours_detail[state_name], "Unsch")
        )
    )

-- Find the last date with 0 unscheduled hours (streak breaker)
VAR LastZeroDate = 
    MAXX(
        FILTER(DailyUnschHours, [UnschHours] = 0 || ISBLANK([UnschHours])),
        state_hours_detail[state_date]
    )

-- Sum hours from streak start to latest date
VAR ContinuousHours = 
    SUMX(
        FILTER(
            DailyUnschHours, 
            state_hours_detail[state_date] > LastZeroDate || ISBLANK(LastZeroDate)
        ),
        [UnschHours]
    )

RETURN
    IF(ISBLANK(CurrentEntity), BLANK(), ContinuousHours)









Tools_Continuous_Unsch_Down_12Plus = 
VAR LatestDate = CALCULATE(MAX(state_hours_detail[state_date]), ALL(state_hours_detail[state_date]))
VAR EntitiesWithContinuousDown =
    ADDCOLUMNS(
        SUMMARIZE(
            FILTER(
                ALL(state_hours_detail),
                NOT(CONTAINSSTRING(state_hours_detail[ENTITY], "_LP")) &&
                NOT(CONTAINSSTRING(state_hours_detail[ENTITY], "_VTM")) &&
                NOT(CONTAINSSTRING(state_hours_detail[ENTITY], "_LLM"))
            ),
            state_hours_detail[FAB_ENTITY]
        ),
        "ContinuousHours", [Continuous_Unscheduled_Down_Hours]
    )
RETURN
    COUNTROWS(
        FILTER(
            EntitiesWithContinuousDown,
            [ContinuousHours] >= 12
        )
    )













Tools_Continuous_Unsch_Down_24Plus = 
VAR LatestDate = CALCULATE(MAX(state_hours_detail[state_date]), ALL(state_hours_detail[state_date]))
VAR EntitiesWithContinuousDown =
    ADDCOLUMNS(
        SUMMARIZE(
            FILTER(
                ALL(state_hours_detail),
                NOT(CONTAINSSTRING(state_hours_detail[ENTITY], "_LP")) &&
                NOT(CONTAINSSTRING(state_hours_detail[ENTITY], "_VTM")) &&
                NOT(CONTAINSSTRING(state_hours_detail[ENTITY], "_LLM"))
            ),
            state_hours_detail[FAB_ENTITY]
        ),
        "ContinuousHours", [Continuous_Unscheduled_Down_Hours]
    )
RETURN
    COUNTROWS(
        FILTER(
            EntitiesWithContinuousDown,
            [ContinuousHours] >= 24
        )
    )
