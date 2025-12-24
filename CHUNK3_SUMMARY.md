# CHUNK 3 COMPLETE: Bronze Layer Ingestion

## Files Created

### 1. entity_states_ingestion.py
**Purpose:** Load EntityStates.csv files from work week folders into Bronze layer

**Key Features:**
- Discovers EntityStates.csv files in WW folders
- Supports full refresh (last 4 weeks) and incremental (current week only)
- Validates required columns: FAB, ENTITY, ENTITY_STATE, HOURS_IN_STATE
- Applies entity normalization (PC -> PM conversion)
- Creates FAB_ENTITY composite key
- Adds metadata columns: source_file, load_ww, load_ts, load_date
- Comprehensive error handling and logging

**Main Class:** EntityStatesIngestion
**Entry Point:** run_entity_states_ingestion(config, mode)

**Modes:**
- full: Load last N weeks (configurable, default 4)
- incremental: Load current week only

### 2. counters_ingestion.py
**Purpose:** Load Counters_*.csv files from work week folders into Bronze layer

**Key Features:**
- Finds LATEST Counters file in each WW folder BY MODIFIED DATE
- Adjusts timestamp by subtracting 1 day from file modified time
- Supports full refresh (last 4 weeks) and incremental (current week only)
- Handles dynamic part counter columns (different tools have different counters)
- Applies entity normalization (PC -> PM conversion)
- Creates FAB_ENTITY composite key
- Adds metadata columns: source_file, load_ww, load_ts, counter_date, file_modified_ts
- Logs total part counter columns found

**Main Class:** CountersIngestion
**Entry Point:** run_counters_ingestion(config, mode)

**File Selection Logic:**
```
For each WW folder:
1. Find all files matching "Counters_*.csv"
2. Get file modified timestamp for each
3. Sort by modified time (most recent first)
4. Select the most recent file
5. Adjust timestamp by -1 day
6. Use adjusted date as counter_date
```

**Example:**
```
Folder: 2025WW51
Files:
  - Counters_2025-11-29.csv (modified: 2025-12-18 03:15:23)  <- SELECTED
  - Counters_2025-11-28.csv (modified: 2025-12-17 03:10:11)
  - Counters_2025-12-16.csv (modified: 2025-12-16 03:08:45)

Result:
  - Selected: Counters_2025-11-29.csv
  - Modified: 2025-12-18 03:15:23
  - Adjusted: 2025-12-17 (subtract 1 day)
  - counter_date = 2025-12-17
```

## Bronze Layer Output Tables

### entity_states_raw
**Columns:**
- FAB
- WW
- DAY_SHIFT
- ENTITY_STATE
- ENTITY (normalized: PC -> PM)
- HOURS_IN_STATE
- Total_Hours
- % in State
- FAB_ENTITY (composite key)
- source_file
- load_ww
- load_ts
- load_date

**Grain:** One row per entity per state per shift per day

### counters_raw
**Columns:**
- ENTITY (normalized: PC -> PM)
- FAB (derived if not present)
- FAB_ENTITY (composite key)
- [Dynamic part counter columns - varies by tool type]
- source_file
- load_ww
- load_ts
- counter_date (adjusted timestamp)
- file_modified_ts

**Grain:** One row per entity per counter date

## FAB Column Handling

**EntityStates:** FAB column is present in source file

**Counters:** FAB column may NOT be present in source file
- Current implementation: Extract from first part of ENTITY (before underscore)
- Example: "F28_GTA101_PM" -> FAB = "F28"
- May need refinement based on actual data

**Note:** This may need to be updated if Counters files have a different structure

## Data Flow

```
Work Week Folders (Network Share)
  |
  v
[Bronze Ingestion]
  - EntityStates: Direct file match
  - Counters: Latest by modified date
  |
  v
Bronze Tables (SQL Server)
  - entity_states_raw
  - counters_raw
```

## Testing

Both modules include standalone test scripts:

```bash
# Test EntityStates ingestion
python entity_states_ingestion.py full

# Test Counters ingestion  
python counters_ingestion.py incremental
```

## Configuration Used

From config.yaml:
```yaml
entity_counters_source:
  root_path: "\\teais6303\ES_I-Pro\Data_Analytics\Data\PM_Flex"
  entity_states:
    file_name: "EntityStates.csv"
  counters:
    file_prefix: "Counters_"
    date_adjustment_days: -1

historical_load:
  enabled: true
  weeks_to_load: 4

entity_normalization:
  replace_pc_with_pm: true
  pattern: "_PC"
  replacement: "_PM"
```

## Next: Chunk 4 - Silver Layer (Wafer Production)

Will create:
- wafer_production.py
  - Counter keyword search (Focus -> APCCounter -> ESCCounter -> PMACounter)
  - Part replacement detection (threshold: -1000)
  - Fallback logic when primary counter fails
  - Wafers per running hour calculation
  - Comprehensive logging of all decisions
