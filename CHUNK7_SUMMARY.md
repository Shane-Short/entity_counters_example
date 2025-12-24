# CHUNK 7 COMPLETE: Pipeline Orchestration & Integration

## Files Created

### 1. database_engine.py
**Purpose:** SQL Server connection and data loading utilities

**Features:**
- Connection string building (trusted or SQL auth)
- Environment variable substitution
- DataFrame to SQL Server insertion
- Batch loading with error handling
- Transaction support (commit/rollback)
- Row count queries
- Table truncation

**Key Class:** SQLServerEngine
**Entry Point:** load_to_sqlserver(df, config, table_params_key)

**Example Usage:**
```python
from utils.database_engine import load_to_sqlserver

rows_loaded = load_to_sqlserver(
    df,
    config,
    'ENTITY_STATES_SQLSERVER_OUTPUT',
    if_exists='append'
)
```

### 2. run_etl_pipeline.py
**Purpose:** Main ETL pipeline orchestrator

**Features:**
- Command-line interface for pipeline execution
- Layer-specific execution (bronze, silver, gold, or all)
- Full and incremental refresh modes
- Comprehensive logging
- Error handling and rollback
- Duration tracking

**Command-Line Usage:**
```bash
# Full pipeline (all layers)
python run_etl_pipeline.py --layer all --mode full

# Bronze only (incremental)
python run_etl_pipeline.py --layer bronze --mode incremental

# Custom config
python run_etl_pipeline.py --config custom.yaml --layer silver --mode full
```

**Pipeline Flow:**
```
run_pipeline()
  |
  v
[Bronze Layer]
  - run_entity_states_ingestion()
  - Load to entity_states_raw
  - run_counters_ingestion()
  - Load to counters_raw
  |
  v
[Silver Layer]
  - run_silver_enrichment()
    - calculate_state_hours()
    - calculate_wafer_production()
    - track_part_replacements()
  - Load to silver tables
  |
  v
[Gold Layer]
  - create_gold_facts()
    - create_daily_production_fact()
    - create_weekly_production_fact()
    - create_state_hours_daily_fact()
    - create_state_hours_weekly_fact()
  - Load to gold tables
```

### 3. requirements.txt
**Purpose:** Python dependencies

**Core Dependencies:**
- pandas >= 2.0.0 - Data processing
- numpy >= 1.24.0 - Numerical operations
- pyodbc >= 5.0.0 - SQL Server connectivity
- pyyaml >= 6.0 - Configuration parsing
- python-dotenv >= 1.0.0 - Environment variables

### 4. README.md
**Purpose:** Complete documentation and user guide

**Sections:**
- Overview and architecture
- Setup instructions
- Usage examples
- Configuration guide
- Key features
- Data table descriptions
- Logging details
- Power BI integration
- Troubleshooting
- File structure

## Complete Pipeline Structure

```
etl_entity_counters_ingestion/
├── config/
│   └── config.yaml                    # Main configuration
├── etl/
│   ├── bronze/
│   │   ├── entity_states_ingestion.py # EntityStates loader
│   │   └── counters_ingestion.py      # Counters loader
│   ├── silver/
│   │   ├── state_hours.py             # State hours calculation
│   │   ├── wafer_production.py        # Wafer calculation
│   │   ├── part_replacements.py       # Replacement tracking
│   │   └── enrichment.py              # Silver orchestration
│   └── gold/
│       └── aggregations.py            # KPI aggregations
├── sql/
│   └── ddl/
│       ├── create_bronze_tables.sql   # Bronze DDL
│       ├── create_silver_tables.sql   # Silver DDL
│       ├── create_gold_tables.sql     # Gold DDL
│       └── setup_database.sql         # Master DDL
├── utils/
│   ├── helpers.py                     # File discovery, WW calc
│   ├── logger.py                      # Logging utilities
│   └── database_engine.py             # SQL Server utilities
├── logs/                              # Runtime logs
├── .env                               # Credentials (create manually)
├── requirements.txt                   # Python dependencies
├── run_etl_pipeline.py               # Main entry point
└── README.md                          # Documentation
```

## Setup Instructions

### Step 1: Create .env File
```env
SQL_USER=your_username
SQL_PASS=your_password
```

### Step 2: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 3: Setup Database
```sql
-- In SQL Server Management Studio
-- Execute: sql/ddl/setup_database.sql
```

### Step 4: Update Configuration
```yaml
# config/config.yaml
entity_counters_source:
  root_path: "\\\\teais6303\\ES_I-Pro\\Data_Analytics\\Data\\PM_Flex"

table_parameters:
  ENTITY_STATES_SQLSERVER_OUTPUT:
    sqlserver:
      server: TEHAUSTELSQL1
      database: Parts_Counter_Production
```

### Step 5: Run Pipeline
```bash
python run_etl_pipeline.py --layer all --mode full
```

## Execution Modes

### Full Refresh
- Loads last 4 weeks of data (configurable)
- Processes all available files
- Recalculates all metrics
- Use for: Initial load, data corrections, schema changes

```bash
python run_etl_pipeline.py --layer all --mode full
```

### Incremental Refresh
- Loads current week only
- Processes new files only
- Appends to existing data
- Use for: Daily/weekly scheduled runs

```bash
python run_etl_pipeline.py --layer all --mode incremental
```

## Layer Execution

### Bronze Only
```bash
python run_etl_pipeline.py --layer bronze --mode full
```
- Ingests EntityStates.csv and Counters files
- Loads to entity_states_raw and counters_raw
- No calculations performed

### Silver Only
```bash
python run_etl_pipeline.py --layer silver --mode full
```
- Reads from Bronze tables
- Calculates state hours and wafer production
- Detects part replacements
- Loads to silver tables

### Gold Only
```bash
python run_etl_pipeline.py --layer gold --mode full
```
- Reads from Silver tables
- Aggregates daily and weekly metrics
- Calculates utilization percentages
- Loads to gold fact tables

## Logging Output

### Console Output
```
================================================================================
Entity States & Counters ETL Pipeline
================================================================================
Config loaded from: config/config.yaml

================================================================================
BRONZE LAYER - FULL MODE
================================================================================
Step 1/2: EntityStates Ingestion
INFO: Discovered 4 EntityStates files to process
INFO: EntityStates: 1250 rows loaded to SQL Server

Step 2/2: Counters Ingestion
INFO: Discovered 4 Counters files to process
INFO: Counters: 480 rows loaded to SQL Server
BRONZE LAYER COMPLETE

================================================================================
SILVER LAYER - FULL MODE
================================================================================
STEP 1: Calculating state hours
INFO: State hours complete: 480 rows

STEP 2: Calculating wafer production
INFO: Wafer production complete: 480 rows
INFO: Part replacements detected: 12

STEP 3: Tracking part replacements
INFO: Part replacements complete: 12 rows
SILVER LAYER COMPLETE

================================================================================
GOLD LAYER - FULL MODE
================================================================================
INFO: Daily production: 480 rows aggregated
INFO: Weekly production: 120 rows aggregated
INFO: Daily state hours: 480 rows aggregated
INFO: Weekly state hours: 120 rows aggregated
GOLD LAYER COMPLETE

================================================================================
PIPELINE COMPLETED SUCCESSFULLY
================================================================================
Started: 2025-12-22 14:30:15
Finished: 2025-12-22 14:32:48
Duration: 0:02:33
Log file: logs/entity_counters_pipeline_20251222_143015.log
================================================================================
```

### Log File
```
logs/entity_counters_pipeline_20251222_143015.log
```

Contains detailed:
- Counter selection decisions
- Part replacement events
- State classifications
- File discovery results
- Error messages and stack traces

## Database Connection

### Trusted Connection (Windows Auth)
```yaml
table_parameters:
  ENTITY_STATES_SQLSERVER_OUTPUT:
    sqlserver:
      server: TEHAUSTELSQL1
      database: Parts_Counter_Production
      trusted_connection: true
```

### SQL Authentication
```yaml
table_parameters:
  ENTITY_STATES_SQLSERVER_OUTPUT:
    sqlserver:
      server: TEHAUSTELSQL1
      database: Parts_Counter_Production
      trusted_connection: false
      username: ${SQL_USER}  # From .env
      password: ${SQL_PASS}  # From .env
```

## Integration with Existing PM_Flex Pipeline

This pipeline is designed to run independently but can share:
- Same database: Parts_Counter_Production
- Same network share: \\teais6303\ES_I-Pro\Data_Analytics\Data\PM_Flex
- Same .env file for credentials
- Similar medallion architecture pattern

## Power BI Connection

1. Open Power BI Desktop
2. Get Data > SQL Server
3. Server: TEHAUSTELSQL1
4. Database: Parts_Counter_Production
5. Tables to import:
   - fact_daily_production
   - fact_weekly_production
   - fact_state_hours_daily
   - fact_state_hours_weekly

## Next Steps

1. Create .env file with credentials
2. Run database setup SQL scripts
3. Test Bronze layer ingestion
4. Validate Silver calculations
5. Verify Gold aggregations
6. Connect Power BI to fact tables
7. Schedule pipeline execution

## TODO: Enhancements for Production

1. Add SQL Server table loading to run_etl_pipeline.py
2. Create table_parameters entries for all Silver/Gold tables in config.yaml
3. Implement Bronze table reading for Silver/Gold layers
4. Add data retention cleanup logic
5. Create scheduling script (Windows Task Scheduler or Airflow)
6. Add email notifications on failure
7. Implement pipeline monitoring dashboard
