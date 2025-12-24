# CHUNK 5 COMPLETE: Silver Enrichment & Gold Aggregations

## Files Created

### 1. part_replacements.py
**Purpose:** Track all part replacement events

**Features:**
- Extracts rows where part_replacement_detected = TRUE
- Records replacement date, counter name, values before/after
- Calculates value drop and part wafers at replacement
- Deduplicates on (ENTITY + replacement_date + part_counter_name)

**Output Columns:**
- ENTITY
- replacement_date
- part_counter_name
- last_value_before_replacement
- first_value_after_replacement
- value_drop
- part_wafers_at_replacement
- replacement_detected_ts
- notes

**Example Record:**
```
ENTITY: F28_GTA101_PM
replacement_date: 2025-12-18
part_counter_name: FocusRingCounter
last_value_before_replacement: 17500
first_value_after_replacement: 250
value_drop: 17250
part_wafers_at_replacement: 17500
notes: Part replacement: FocusRingCounter reset; Used fallback: APCCounter
```

### 2. enrichment.py
**Purpose:** Orchestrate all Silver layer calculations

**Process Flow:**
```
Step 1: Calculate State Hours
  Input: entity_states_raw (Bronze)
  Output: state_hours (Silver)
  
Step 2: Calculate Wafer Production
  Input: counters_raw (Bronze) + state_hours (Silver)
  Output: wafer_production (Silver)
  
Step 3: Track Part Replacements
  Input: wafer_production (Silver)
  Output: part_replacements (Silver)
```

**Entry Point:** run_silver_enrichment(config, entity_states_df, counters_df)

**Returns:** (state_hours, wafer_production, part_replacements)

### 3. aggregations.py
**Purpose:** Create Gold layer KPI tables for Power BI

**Fact Tables Created:**

#### fact_daily_production
**Grain:** One row per entity per day

**Columns:**
- ENTITY, FAB, FAB_ENTITY
- production_date
- wafers_produced
- wafers_per_hour
- running_hours, idle_hours, down_hours, bagged_hours, total_hours
- is_bagged (boolean)
- part_replacement_detected (boolean)
- counter_column_used, counter_keyword_used
- calculation_timestamp

**Dedup Key:** (ENTITY + production_date)

#### fact_weekly_production
**Grain:** One row per entity per work week

**Columns:**
- ENTITY, FAB, FAB_ENTITY, YEARWW
- total_wafers_produced
- total_running_hours, total_idle_hours, total_down_hours, total_bagged_hours, total_hours
- avg_wafers_per_hour
- part_replacements_count
- week_start_date, week_end_date, days_with_data
- calculation_timestamp

**Calculated Metrics:**
- avg_wafers_per_hour = total_wafers_produced / total_running_hours
- Aggregates all daily metrics by week

**Dedup Key:** (ENTITY + YEARWW)

#### fact_state_hours_daily
**Grain:** One row per entity per day

**Columns:**
- ENTITY, FAB, FAB_ENTITY
- state_date
- running_hours, idle_hours, down_hours, bagged_hours, total_hours
- running_pct, idle_pct, down_pct (utilization percentages)
- is_bagged (boolean)
- calculation_timestamp

**Calculated Metrics:**
- running_pct = (running_hours / total_hours) * 100
- idle_pct = (idle_hours / total_hours) * 100
- down_pct = (down_hours / total_hours) * 100

**Dedup Key:** (ENTITY + state_date)

#### fact_state_hours_weekly
**Grain:** One row per entity per work week

**Columns:**
- ENTITY, FAB, FAB_ENTITY, YEARWW
- total_running_hours, total_idle_hours, total_down_hours, total_bagged_hours, total_hours
- running_pct, idle_pct, down_pct (weekly utilization percentages)
- was_bagged_any_day (boolean - TRUE if bagged any day in the week)
- week_start_date, week_end_date, days_with_data
- calculation_timestamp

**Dedup Key:** (ENTITY + YEARWW)

## Complete Data Pipeline Flow

```
BRONZE LAYER
  entity_states_raw
  counters_raw
     |
     v
SILVER LAYER
  Step 1: state_hours
     |
     v
  Step 2: wafer_production (uses state_hours)
     |
     v
  Step 3: part_replacements (extracts from wafer_production)
     |
     v
GOLD LAYER
  fact_daily_production (merges wafer_production + state_hours)
  fact_weekly_production (aggregates daily)
  fact_state_hours_daily (from state_hours)
  fact_state_hours_weekly (aggregates daily)
```

## Table Summary

### Bronze (Raw Data)
- entity_states_raw
- counters_raw

### Silver (Enriched Data)
- state_hours
- wafer_production
- part_replacements

### Gold (Aggregated KPIs)
- fact_daily_production
- fact_weekly_production
- fact_state_hours_daily
- fact_state_hours_weekly

**Total Tables: 9** (2 Bronze + 3 Silver + 4 Gold)

## Deduplication Summary

All tables implement deduplication before database insert:

| Layer | Table | Dedup Key | Method |
|-------|-------|-----------|--------|
| Bronze | entity_states_raw | FAB_ENTITY + DAY_SHIFT + ENTITY_STATE | drop_duplicates(keep='last') |
| Bronze | counters_raw | FAB_ENTITY + counter_date | drop_duplicates(keep='last') |
| Silver | state_hours | ENTITY + state_date | drop_duplicates(keep='last') |
| Silver | wafer_production | ENTITY + counter_date | drop_duplicates(keep='last') |
| Silver | part_replacements | ENTITY + replacement_date + part_counter_name | drop_duplicates(keep='last') |
| Gold | fact_daily_production | ENTITY + production_date | drop_duplicates(keep='last') |
| Gold | fact_weekly_production | ENTITY + YEARWW | drop_duplicates(keep='last') |
| Gold | fact_state_hours_daily | ENTITY + state_date | drop_duplicates(keep='last') |
| Gold | fact_state_hours_weekly | ENTITY + YEARWW | drop_duplicates(keep='last') |

## Key Metrics for Power BI

### Production Metrics
- Wafers produced per day/week
- Wafers per running hour
- Part replacement frequency
- Counter usage tracking

### Utilization Metrics
- Running hours and percentage
- Idle hours and percentage
- Down hours and percentage
- Bagged status tracking

### Part Lifecycle Metrics
- Replacement dates
- Wafers at replacement
- Part lifespan tracking

## Next: Chunk 6 - SQL DDL & Database Setup

Will create:
- create_bronze_tables.sql
- create_silver_tables.sql
- create_gold_tables.sql
- Database initialization scripts
