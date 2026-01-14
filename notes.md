// ============================================================
// GROUP 1: UTILIZATION MEASURES (Foundation)
// ============================================================

Total_Running_Hours = 
SUM(state_hours[running_hours])

// ------------------------------------------------------------

Total_Idle_Hours = 
SUM(state_hours[idle_hours])

// ------------------------------------------------------------

Total_Down_Hours = 
SUM(state_hours[down_hours])

// ------------------------------------------------------------

Total_Bagged_Hours = 
SUM(state_hours[bagged_hours])

// ------------------------------------------------------------

Total_State_Hours = 
SUM(state_hours[total_hours])

// ------------------------------------------------------------

Utilization_% = 
DIVIDE(
    [Total_Running_Hours],
    [Total_State_Hours],
    0
)

// ------------------------------------------------------------

Availability_% = 
DIVIDE(
    [Total_Running_Hours] + [Total_Idle_Hours],
    [Total_State_Hours],
    0
)

// ------------------------------------------------------------

Non_Productive_Hours = 
[Total_Idle_Hours] + [Total_Down_Hours] + [Total_Bagged_Hours]


// ============================================================
// GROUP 2: PRODUCTION MEASURES (Foundation)
// ============================================================

Total_Wafers_Produced = 
SUM(wafer_production[wafers_produced])

// ------------------------------------------------------------

Avg_Daily_Wafers = 
AVERAGEX(
    VALUES(wafer_production[counter_date]),
    CALCULATE(SUM(wafer_production[wafers_produced]))
)

// ------------------------------------------------------------

Avg_Wafers_Per_Hour = 
DIVIDE(
    [Total_Wafers_Produced],
    [Total_Running_Hours],
    0
)

// ------------------------------------------------------------

Part_Replacement_Count = 
COUNTROWS(part_replacements)

// ------------------------------------------------------------

Weekly_Wafer_Total = 
CALCULATE(
    SUM(fact_weekly_production[wafers_produced])
)

// ------------------------------------------------------------

Production_Gap_Days_Max = 
MAXX(
    wafer_production,
    DATEDIFF(wafer_production[previous_counter_date], wafer_production[counter_date], DAY)
)


// ============================================================
// GROUP 3: ESCALATION MEASURES (12+ and 24+ Hour Alerts)
// ============================================================

// Note: These identify tools currently down that have crossed the threshold
// They check for the most recent date per tool and whether it's still down

Tools_Down_12Plus = 
VAR LatestDate = MAX(state_hours[state_date])
VAR ToolsCurrentlyDown =
    CALCULATETABLE(
        SUMMARIZE(
            state_hours,
            state_hours[FAB_ENTITY],
            "LatestDownHours", CALCULATE(SUM(state_hours[down_hours]), state_hours[state_date] = LatestDate)
        ),
        state_hours[state_date] = LatestDate
    )
RETURN
    COUNTROWS(
        FILTER(
            ToolsCurrentlyDown,
            [LatestDownHours] >= 12
        )
    )

// ------------------------------------------------------------

Tools_Down_24Plus = 
VAR LatestDate = MAX(state_hours[state_date])
VAR ToolsCurrentlyDown =
    CALCULATETABLE(
        SUMMARIZE(
            state_hours,
            state_hours[FAB_ENTITY],
            "LatestDownHours", CALCULATE(SUM(state_hours[down_hours]), state_hours[state_date] = LatestDate)
        ),
        state_hours[state_date] = LatestDate
    )
RETURN
    COUNTROWS(
        FILTER(
            ToolsCurrentlyDown,
            [LatestDownHours] >= 24
        )
    )

// ------------------------------------------------------------

// Table measure for escalation details (use in table visual)
Current_Down_Hours = 
VAR LatestDate = MAX(state_hours[state_date])
RETURN
    CALCULATE(
        SUM(state_hours[down_hours]),
        state_hours[state_date] = LatestDate
    )

// ------------------------------------------------------------

Is_Currently_Down_12Plus = 
IF([Current_Down_Hours] >= 12, "Yes", "No")

// ------------------------------------------------------------

Is_Currently_Down_24Plus = 
IF([Current_Down_Hours] >= 24, "Yes", "No")


// ============================================================
// GROUP 4: PART LIFE MEASURES
// ============================================================

Avg_Part_Life_Wafers = 
AVERAGE(part_replacements[part_wafers_at_replacement])

// ------------------------------------------------------------

// Average part life by part type (for comparison)
Avg_Part_Life_By_Type = 
AVERAGEX(
    VALUES(part_replacements[part_counter_name]),
    CALCULATE(AVERAGE(part_replacements[part_wafers_at_replacement]))
)

// ------------------------------------------------------------

// For individual part replacement vs fleet average
Part_Life_vs_Avg_% = 
VAR CurrentPartType = SELECTEDVALUE(part_replacements[part_counter_name])
VAR FleetAvgForType = 
    CALCULATE(
        AVERAGE(part_replacements[part_wafers_at_replacement]),
        ALL(part_replacements),
        part_replacements[part_counter_name] = CurrentPartType
    )
VAR CurrentAvg = AVERAGE(part_replacements[part_wafers_at_replacement])
RETURN
    DIVIDE(CurrentAvg, FleetAvgForType, 0)

// ------------------------------------------------------------

Below_Avg_Part_Replacements = 
VAR PartsWithStatus =
    ADDCOLUMNS(
        part_replacements,
        "FleetAvg", 
            VAR ThisPart = part_replacements[part_counter_name]
            RETURN CALCULATE(
                AVERAGE(part_replacements[part_wafers_at_replacement]),
                ALL(part_replacements),
                part_replacements[part_counter_name] = ThisPart
            )
    )
RETURN
    COUNTROWS(
        FILTER(
            PartsWithStatus,
            part_replacements[part_wafers_at_replacement] < [FleetAvg] * 0.5
        )
    )

// ------------------------------------------------------------

Above_Avg_Part_Replacements = 
VAR PartsWithStatus =
    ADDCOLUMNS(
        part_replacements,
        "FleetAvg", 
            VAR ThisPart = part_replacements[part_counter_name]
            RETURN CALCULATE(
                AVERAGE(part_replacements[part_wafers_at_replacement]),
                ALL(part_replacements),
                part_replacements[part_counter_name] = ThisPart
            )
    )
RETURN
    COUNTROWS(
        FILTER(
            PartsWithStatus,
            part_replacements[part_wafers_at_replacement] > [FleetAvg] * 1.5
        )
    )

// ------------------------------------------------------------

// Part life status category for conditional formatting
Part_Life_Status = 
VAR CurrentLife = AVERAGE(part_replacements[part_wafers_at_replacement])
VAR CurrentPartType = SELECTEDVALUE(part_replacements[part_counter_name])
VAR FleetAvgForType = 
    CALCULATE(
        AVERAGE(part_replacements[part_wafers_at_replacement]),
        ALL(part_replacements),
        part_replacements[part_counter_name] = CurrentPartType
    )
VAR Ratio = DIVIDE(CurrentLife, FleetAvgForType, 0)
RETURN
    SWITCH(
        TRUE(),
        Ratio < 0.5, "Early",
        Ratio > 1.5, "Extended",
        "Normal"
    )

// ------------------------------------------------------------

Days_Since_Last_Counter = 
AVERAGE(
    DATEDIFF(wafer_production[previous_counter_date], wafer_production[counter_date], DAY)
)


// ============================================================
// GROUP 5: SITE ANALYSIS MEASURES
// ============================================================

Site_Total_PM_Events = 
SUM(fact_pm_kpis_by_site_ww[total_pm_events])

// ------------------------------------------------------------

Site_Unscheduled_Count = 
SUM(fact_pm_kpis_by_site_ww[unscheduled_pm_count])

// ------------------------------------------------------------

Site_Unscheduled_Rate = 
DIVIDE(
    [Site_Unscheduled_Count],
    [Site_Total_PM_Events],
    0
)

// ------------------------------------------------------------

Fleet_Avg_Unscheduled_Rate = 
VAR TotalUnscheduled = 
    CALCULATE(
        SUM(fact_pm_kpis_by_site_ww[unscheduled_pm_count]),
        ALL(fact_pm_kpis_by_site_ww)
    )
VAR TotalPMs = 
    CALCULATE(
        SUM(fact_pm_kpis_by_site_ww[total_pm_events]),
        ALL(fact_pm_kpis_by_site_ww)
    )
RETURN
    DIVIDE(TotalUnscheduled, TotalPMs, 0)

// ------------------------------------------------------------

Site_vs_Fleet_Delta = 
[Site_Unscheduled_Rate] - [Fleet_Avg_Unscheduled_Rate]

// ------------------------------------------------------------

Fleet_Avg_PM_Life = 
CALCULATE(
    AVERAGE(pm_flex_enriched[Attribute_Value]),
    ALL(pm_flex_enriched)
)

// ------------------------------------------------------------

Site_Avg_PM_Life = 
AVERAGE(pm_flex_enriched[Attribute_Value])

// ------------------------------------------------------------

Site_PM_Life_vs_Fleet = 
[Site_Avg_PM_Life] - [Fleet_Avg_PM_Life]

// ------------------------------------------------------------

Site_Daily_Wafers_Avg = 
AVERAGEX(
    VALUES(wafer_production[counter_date]),
    CALCULATE(SUM(wafer_production[wafers_produced]))
)

// ------------------------------------------------------------

Site_Chronic_Tools_Count = 
CALCULATE(
    DISTINCTCOUNT(pm_flex_chronic_tools[ENTITY]),
    pm_flex_chronic_tools[chronic_flag] = TRUE()
)


// ============================================================
// GROUP 6: RANKING MEASURES (for Site Summary comparisons)
// ============================================================

Site_Rank_Unscheduled = 
RANKX(
    ALL(DimTools[FACILITY]),
    [Site_Unscheduled_Rate],
    ,
    DESC,
    Dense
)

// ------------------------------------------------------------

Site_Rank_PM_Life = 
RANKX(
    ALL(DimTools[FACILITY]),
    [Site_Avg_PM_Life],
    ,
    DESC,
    Dense
)

// ------------------------------------------------------------

Site_Rank_Utilization = 
RANKX(
    ALL(DimTools[FACILITY]),
    [Utilization_%],
    ,
    DESC,
    Dense
)

// ------------------------------------------------------------

// Rank for worst down time (ascending - most down hours = rank 1)
Site_Rank_Down_Hours = 
RANKX(
    ALL(DimTools[FACILITY]),
    [Total_Down_Hours],
    ,
    DESC,
    Dense
)


// ============================================================
// GROUP 7: UTILITY / HELPER MEASURES
// ============================================================

// Count of unique tools in current context
Tool_Count = 
DISTINCTCOUNT(DimTools[FAB_ENTITY])

// ------------------------------------------------------------

// Count of unique tools with production data
Tools_With_Production = 
DISTINCTCOUNT(wafer_production[FAB_ENTITY])

// ------------------------------------------------------------

// Count of unique tools with state data
Tools_With_State_Data = 
DISTINCTCOUNT(state_hours[FAB_ENTITY])

// ------------------------------------------------------------

// Last data refresh date (for display on reports)
Last_State_Data_Date = 
MAX(state_hours[state_date])

// ------------------------------------------------------------

Last_Production_Data_Date = 
MAX(wafer_production[counter_date])

// ------------------------------------------------------------

Last_Replacement_Date = 
MAX(part_replacements[replacement_date])
