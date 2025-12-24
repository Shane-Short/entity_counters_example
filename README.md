# Entity States & Counters ETL Pipeline

Unified ETL pipeline for ingesting EntityStates.csv and Counters_*.csv files from network share into SQL Server with Bronze/Silver/Gold medallion architecture.

## Overview

This pipeline processes two data sources:
- **EntityStates.csv** - Weekly entity state data (Running, Idle, Down, Bagged)
- **Counters_YYYY_MM_DD.csv** - Daily part counter readings

The pipeline calculates:
- Daily wafer production from part counter changes
- State hours (Running, Idle, Down, Bagged)
- Part replacement detection and tracking
- Utilization metrics and KPIs

## Architecture

```
Network Share (\\teais6303\ES_I-Pro\Data_Analytics\Data\PM_Flex\YYYYWWNN\)
  |
  v
[BRONZE LAYER] - Raw data mirror
  - entity_states_raw
  - counters_raw
  |
  v
[SILVER LAYER] - Enriched data
  - state_hours
  - wafer_production
  - part_replacements
  |
  v
[GOLD LAYER] - KPI fact tables for Power BI
  - fact_daily_production
  - fact_weekly_production
  - fact_state_hours_daily
  - fact_state_hours_weekly
```

## Setup

### 1. Environment Setup

Create `.env` file in project root:

```env
SQL_USER=your_username
SQL_PASS=your_password
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Database Setup

Run SQL scripts to create tables:

```sql
-- Option 1: Run master script
USE Parts_Counter_Production;
GO
-- Execute: sql/ddl/setup_database.sql

-- Option 2: Run individual scripts
-- Execute in order:
1. sql/ddl/create_bronze_tables.sql
2. sql/ddl/create_silver_tables.sql
3. sql/ddl/create_gold_tables.sql
```

### 4. Configuration

Edit `config/config.yaml` to match your environment:
- Network share paths
- SQL Server connection details
- Business rules (keywords, thresholds, states)

## Usage

### Run Complete Pipeline

```bash
# Full refresh (last 4 weeks)
python run_etl_pipeline.py --layer all --mode full

# Incremental refresh (current week only)
python run_etl_pipeline.py --layer all --mode incremental
```

### Run Individual Layers

```bash
# Bronze layer only
python run_etl_pipeline.py --layer bronze --mode full

# Silver layer only
python run_etl_pipeline.py --layer silver --mode full

# Gold layer only
python run_etl_pipeline.py --layer gold --mode full
```

### Custom Config File

```bash
python run_etl_pipeline.py --config path/to/config.yaml --layer all --mode full
```

## Configuration

### Wafer Production Calculation

```yaml
wafer_production:
  primary_keywords:
    - "Focus"  # FocusRingCounter
  fallback_keywords:
    - "APCCounter"
    - "ESCCounter"
    - "PMACounter"
  part_replacement:
    negative_threshold: -1000
```

### Entity State Classification

```yaml
entity_states:
  running_states:
    - "Running1"
    - "Running2"
    # ... up to Running8
  idle_states:
    - "UpToProduction"
  bagged_state: "Bagged"
```

### Data Retention

```yaml
retention:
  bronze:
    entity_states_raw:
      days: 90
    counters_raw:
      days: 90
  silver:
    state_hours:
      days: 180
    wafer_production:
      days: 180
    part_replacements:
      days: 365
  gold:
    fact_daily_production:
      days: 365
```

## Key Features

### Wafer Production Calculation
- Searches for counter columns with configurable keywords
- Detects part replacements (threshold: -1000)
- Automatic fallback to alternative counters
- Calculates wafers per running hour

### Part Replacement Tracking
- Records replacement date, part name, values
- Tracks part lifecycle (wafers at replacement)
- Historical replacement analysis

### State Hours Calculation
- Classifies entity states (Running/Idle/Down/Bagged)
- Aggregates hours by entity and date
- Calculates utilization percentages

### Duplicate Prevention
- All tables implement deduplication
- Natural key constraints in database
- Python-level deduplication before insert

## Data Tables

### Bronze Layer (Raw Data)
- `entity_states_raw` - Raw EntityStates data
- `counters_raw` - Raw Counters data

### Silver Layer (Enriched Data)
- `state_hours` - Daily state hours by entity
- `wafer_production` - Daily wafer production metrics
- `part_replacements` - Part replacement events

### Gold Layer (KPI Tables)
- `fact_daily_production` - Daily production + utilization
- `fact_weekly_production` - Weekly aggregated production
- `fact_state_hours_daily` - Daily state hours + utilization %
- `fact_state_hours_weekly` - Weekly aggregated state hours

## Logging

Logs are written to:
- `logs/entity_counters_pipeline_YYYYMMDD_HHMMSS.log`

Log levels:
- INFO - Normal operation
- WARNING - Missing data, fallback usage
- ERROR - Failures, exceptions

Special logging:
- Counter selection decisions
- Part replacement events
- Unknown entity states
- Bagged tool identification

## Power BI Integration

Connect to Gold layer tables:
1. Open Power BI Desktop
2. Get Data > SQL Server
3. Server: TEHAUSTELSQL1
4. Database: Parts_Counter_Production
5. Select fact tables:
   - fact_daily_production
   - fact_weekly_production
   - fact_state_hours_daily
   - fact_state_hours_weekly

## Troubleshooting

### No data loaded
- Check network share path in config.yaml
- Verify WW folders exist
- Check file names match patterns

### Connection errors
- Verify SQL Server credentials in .env
- Check server name and database
- Ensure ODBC Driver 18 is installed

### Part counter not found
- Check counter keywords in config.yaml
- Review logs for "No counter found" warnings
- Add additional fallback keywords

### Duplicate rows
- Pipeline includes automatic deduplication
- Check unique constraints in database
- Review logs for deduplication counts

## File Structure

```
etl_entity_counters_ingestion/
├── config/
│   └── config.yaml
├── etl/
│   ├── bronze/
│   │   ├── entity_states_ingestion.py
│   │   └── counters_ingestion.py
│   ├── silver/
│   │   ├── state_hours.py
│   │   ├── wafer_production.py
│   │   ├── part_replacements.py
│   │   └── enrichment.py
│   └── gold/
│       └── aggregations.py
├── sql/
│   └── ddl/
│       ├── create_bronze_tables.sql
│       ├── create_silver_tables.sql
│       ├── create_gold_tables.sql
│       └── setup_database.sql
├── utils/
│   ├── helpers.py
│   ├── logger.py
│   └── database_engine.py
├── logs/
├── .env
├── requirements.txt
├── run_etl_pipeline.py
└── README.md
```

## Contact

For issues or questions, contact the Data Engineering team.
