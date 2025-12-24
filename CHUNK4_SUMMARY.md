# CHUNK 4 COMPLETE: Silver Layer - Wafer Production & State Hours

## Files Created

### 1. wafer_production.py
**Purpose:** Calculate daily wafer production from part counter changes

**Key Algorithm:**
```
For each entity, for each day:
1. Search for counter column with keyword (Focus -> APCCounter -> ESCCounter -> PMACounter)
2. Find first counter column with value that matches keyword
3. Calculate change from previous day: current_value - previous_value
4. Detect part replacement: if change < -1000
5. If replacement detected, try fallback counter
6. Calculate: wafers_produced = counter_change
7. Calculate: wafers_per_hour = wafers_produced / running_hours
```

**Counter Keyword Search Logic:**
- Primary: "Focus" (matches FocusRingCounter, TactrasRLSAFocusRingCounter, etc.)
- Fallback 1: "APCCounter"
- Fallback 2: "ESCCounter"
- Fallback 3: "PMACounter"

**Part Replacement Detection:**
- Threshold: -1000 (configurable)
- If counter drops by more than 1000, it's considered a replacement
- Tracks: previous_value, new_value, counter_name, date
- Fallback logic activates to find alternative counter

**Comprehensive Logging:**
- Every counter selection decision
- All negative changes
- Part replacement events
- Fallback usage
- Missing counter warnings
- Final wafer calculations

**Output Columns:**
- ENTITY
- counter_date
- counter_column_used
- counter_keyword_used
- counter_current_value
- counter_previous_value
- counter_change
- part_replacement_detected (boolean)
- wafers_produced
- running_hours
- wafers_per_hour
- calculation_notes

### 2. state_hours.py
**Purpose:** Calculate daily state hours from EntityStates data

**State Classification:**
```
Running: Running1, Running2, Running3, Running4, Running5, Running6, Running7, Running8
Idle: UpToProduction
Bagged: Bagged
Down: Everything else
```

**DAY_SHIFT Parsing:**
- Formats handled: MM/DD-shift, MM/DD/YY-shift, MM/DD/YYYY-shift
- Extracts date portion before dash
- Assumes current year if year not specified

**Aggregation Logic:**
```
For each entity, for each date:
1. Parse DAY_SHIFT to extract date
2. Classify each ENTITY_STATE into Running/Idle/Down/Bagged
3. Sum HOURS_IN_STATE by entity + date + state_category
4. Pivot to create separate columns for each category
5. Calculate total_hours and is_bagged flag
```

**Output Columns:**
- ENTITY
- FAB
- FAB_ENTITY
- state_date
- running_hours
- idle_hours
- down_hours
- bagged_hours
- total_hours
- is_bagged (boolean)

### 3. Updated Bronze Ingestion (Deduplication Added)

**entity_states_ingestion.py:**
- Deduplication key: (FAB_ENTITY + DAY_SHIFT + ENTITY_STATE)
- Method: drop_duplicates(keep='last')
- Applied BEFORE database load

**counters_ingestion.py:**
- Deduplication key: (FAB_ENTITY + counter_date)
- Method: drop_duplicates(keep='last')
- Applied BEFORE database load

## Deduplication Strategy Across All Layers

### Bronze Layer
| Table | Dedup Key | When |
|-------|-----------|------|
| entity_states_raw | FAB_ENTITY + DAY_SHIFT + ENTITY_STATE | Before DB insert |
| counters_raw | FAB_ENTITY + counter_date | Before DB insert |

### Silver Layer
| Table | Dedup Key | When |
|-------|-----------|------|
| state_hours | ENTITY + state_date | After calculation |
| wafer_production | ENTITY + counter_date | After calculation |

### Gold Layer
| Table | Dedup Key | When |
|-------|-----------|------|
| Aggregated tables | ENTITY + date + aggregation_level | After aggregation |

**Why 'keep=last'?**
- Most recent data is typically most accurate
- Handles reprocessing scenarios
- Incremental loads override stale data

## Data Flow

```
Bronze: counters_raw (raw counter values)
   +
   |
   v
Silver: state_hours (running hours calculated)
   +
   |
   v
Silver: wafer_production (wafers/hour calculated)
```

**Dependencies:**
- wafer_production.py REQUIRES state_hours.py output (needs running_hours)
- Must calculate state_hours BEFORE wafer_production

## Example Calculation Walkthrough

**Day 1:**
- Entity: F28_GTA101_PM
- FocusRingCounter: 15,000
- Running hours: 20
- Result: First day, no previous value, cannot calculate

**Day 2:**
- Entity: F28_GTA101_PM
- FocusRingCounter: 17,500
- Running hours: 22
- Counter change: 17,500 - 15,000 = 2,500
- Wafers produced: 2,500
- Wafers per hour: 2,500 / 22 = 113.6

**Day 3 (Part Replacement):**
- Entity: F28_GTA101_PM
- FocusRingCounter: 250 (RESET!)
- Running hours: 18
- Counter change: 250 - 17,500 = -17,250
- Detection: -17,250 < -1000 = TRUE (part replacement)
- Action: Try fallback counter (APCCounter)
- APCCounter current: 45,000, previous: 43,200
- Fallback change: 45,000 - 43,200 = 1,800
- Wafers produced: 1,800
- Wafers per hour: 1,800 / 18 = 100.0
- Note: "Part replacement: FocusRingCounter reset; Used fallback: APCCounter"

## Configuration Used

From config.yaml:
```yaml
wafer_production:
  primary_keywords:
    - "Focus"
  fallback_keywords:
    - "APCCounter"
    - "ESCCounter"
    - "PMACounter"
  part_replacement:
    negative_threshold: -1000
    log_replacements: true
    track_last_value: true

entity_states:
  running_states:
    - "Running1"
    - "Running2"
    - "Running3"
    - "Running4"
    - "Running5"
    - "Running6"
    - "Running7"
    - "Running8"
  idle_states:
    - "UpToProduction"
  bagged_state: "Bagged"
```

## Logging Examples

**Counter Selection:**
```
INFO: Entity F28_GTA101_PM (2025-12-17): Using counter 'FocusRingCounter' (keyword: 'Focus', value: 17500)
```

**Part Replacement:**
```
INFO: PART REPLACEMENT DETECTED - Entity F28_GTA101_PM (2025-12-18): FocusRingCounter dropped from 17500 to 250 (threshold: -1000)
INFO: Entity F28_GTA101_PM (2025-12-18): Fallback counter used - 'Focus' failed (part replacement), using 'APCCounter'
```

**No Counter Found:**
```
WARNING: Entity F11_TEOS03_PM (2025-12-17): No counter found with keywords: ['Focus', 'APCCounter', 'ESCCounter', 'PMACounter']
```

**State Classification:**
```
DEBUG: Entity F28_GTA101_PM (2025-12-17): State hours - Running: 20.00, Idle: 2.50, Down: 1.50
INFO: Entity F24_DRY01_PM (2025-12-17): Tool marked as BAGGED
```

## Next: Chunk 5 - Silver Layer Enrichment & Part Replacements Table

Will create:
- enrichment.py (combine all silver calculations)
- part_replacements.py (track part replacement history)
- Final silver tables ready for Gold aggregation
