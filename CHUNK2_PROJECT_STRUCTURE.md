# Entity States & Counters Pipeline - Project Structure

## Overview
Unified ETL pipeline for EntityStates.csv and Counters_*.csv data ingestion.

## Directory Structure
```
etl_entity_counters_ingestion/
├── .venv/                          # Python virtual environment
├── config/
│   └── config.yaml                 # Main configuration file
├── etl/
│   ├── bronze/
│   │   ├── __init__.py
│   │   ├── entity_states_ingestion.py    # EntityStates.csv loader
│   │   └── counters_ingestion.py         # Counters_*.csv loader
│   ├── silver/
│   │   ├── __init__.py
│   │   ├── wafer_production.py           # Wafer calculation logic
│   │   ├── state_hours.py                # State hours aggregation
│   │   └── enrichment.py                 # General enrichment
│   └── gold/
│       ├── __init__.py
│       └── aggregations.py               # KPI tables
├── sql/
│   └── ddl/
│       ├── create_bronze_tables.sql
│       ├── create_silver_tables.sql
│       └── create_gold_tables.sql
├── utils/
│   ├── __init__.py
│   ├── helpers.py                        # File discovery, WW calc, entity normalization
│   ├── logger.py                         # Logging configuration
│   └── engine.py                         # Database connection utilities
├── logs/                                 # Runtime logs
├── .env                                  # Environment variables
├── requirements.txt                      # Python dependencies
├── run_etl_pipeline.py                  # Main entry point
└── README.md
```

## Key Components Created in Chunk 2

### 1. config/config.yaml
**Business Rules Configuration:**
- Wafer production keywords (Focus → APCCounter → ESCCounter → PMACounter)
- Part replacement threshold (-1000)
- Entity state classifications (Running, Idle, Down, Bagged)
- PC → PM normalization rules
- Historical load (4 weeks)
- Incremental refresh settings
- Retention policies (90/180/365 days)
- Comprehensive logging flags

### 2. utils/helpers.py
**Core Helper Functions:**
- `get_intel_ww()` - Intel fiscal work week calculation
- `get_recent_work_weeks()` - Generate list of recent WW strings
- `find_entity_states_file()` - Locate EntityStates.csv in WW folder
- `find_latest_counters_file()` - Find most recent Counters_*.csv by modified date
- `normalize_entity_name()` - PC → PM entity conversion
- `apply_entity_normalization()` - Apply normalization to DataFrame
- `create_fab_entity_key()` - Create FAB_ENTITY composite key
- `load_csv_safe()` - Safe CSV loading with encoding handling
- `add_metadata_columns()` - Add source_file, load_ww, load_ts
- `adjust_timestamp()` - Timestamp adjustment for Counters file

### 3. utils/logger.py
**Audit Trail Logging:**
- `setup_logger()` - Main logger configuration
- `WaferProductionLogger` - Specialized wafer calculation logging
  - Counter keyword searches
  - Counter selection decisions
  - Negative changes detection
  - Part replacement events
  - Fallback logic tracking
- `StateLogger` - Entity state classification logging
  - Unknown states
  - Bagged tool identification
  - State hour breakdowns
- `create_run_log_file()` - Timestamped log file generation

## Next Chunks

### Chunk 3: Bronze Layer Ingestion
- `etl/bronze/entity_states_ingestion.py`
- `etl/bronze/counters_ingestion.py`

### Chunk 4: Silver Layer - Wafer Production
- `etl/silver/wafer_production.py`
- Counter keyword search logic
- Part replacement detection
- Fallback handling

### Chunk 5: Silver Layer - State Hours & Enrichment
- `etl/silver/state_hours.py`
- `etl/silver/enrichment.py`

### Chunk 6: Gold Layer
- `etl/gold/aggregations.py`

### Chunk 7: SQL DDL
- All table definitions

### Chunk 8: Pipeline Orchestration
- `run_etl_pipeline.py`
- Testing & validation

## Configuration Highlights

### Wafer Production Logic
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
```

### Entity States
```yaml
entity_states:
  running_states:
    - "Running1"
    - "Running2"
    - "Running3"
    - "Running4"
    # ... up to Running8
  idle_states:
    - "UpToProduction"
  bagged_state: "Bagged"
```

### File Discovery
- **EntityStates:** Direct file name match in WW folder
- **Counters:** Finds all `Counters_*.csv`, selects by **most recent modified date**
- **Timestamp Adjustment:** Subtracts 1 day from file modified time
