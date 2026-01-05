# Silver layer - State Hours
  STATE_HOURS_SQLSERVER_OUTPUT:
    target: sqlserver
    sqlserver:
      username: ${SQL_USERNAME}
      password: ${SQL_PASSWORD}
      driver: "ODBC Driver 18 for SQL Server"
      server: TEHAUSTELSQL1
      trusted_connection: false
      database: Parts_Counter_Production
      schema: dbo
      table_name: state_hours
  
  # Silver layer - State Hours Detail
  STATE_HOURS_DETAIL_SQLSERVER_OUTPUT:
    target: sqlserver
    sqlserver:
      username: ${SQL_USERNAME}
      password: ${SQL_PASSWORD}
      driver: "ODBC Driver 18 for SQL Server"
      server: TEHAUSTELSQL1
      trusted_connection: false
      database: Parts_Counter_Production
      schema: dbo
      table_name: state_hours_detail
  
  # Silver layer - Wafer Production
  WAFER_PRODUCTION_SQLSERVER_OUTPUT:
    target: sqlserver
    sqlserver:
      username: ${SQL_USERNAME}
      password: ${SQL_PASSWORD}
      driver: "ODBC Driver 18 for SQL Server"
      server: TEHAUSTELSQL1
      trusted_connection: false
      database: Parts_Counter_Production
      schema: dbo
      table_name: wafer_production
  
  # Silver layer - Part Replacements
  PART_REPLACEMENTS_SQLSERVER_OUTPUT:
    target: sqlserver
    sqlserver:
      username: ${SQL_USERNAME}
      password: ${SQL_PASSWORD}
      driver: "ODBC Driver 18 for SQL Server"
      server: TEHAUSTELSQL1
      trusted_connection: false
      database: Parts_Counter_Production
      schema: dbo
      table_name: part_replacements
  
  # Gold layer - Daily Production
  FACT_DAILY_PRODUCTION_SQLSERVER_OUTPUT:
    target: sqlserver
    sqlserver:
      username: ${SQL_USERNAME}
      password: ${SQL_PASSWORD}
      driver: "ODBC Driver 18 for SQL Server"
      server: TEHAUSTELSQL1
      trusted_connection: false
      database: Parts_Counter_Production
      schema: dbo
      table_name: fact_daily_production
  
  # Gold layer - Weekly Production
  FACT_WEEKLY_PRODUCTION_SQLSERVER_OUTPUT:
    target: sqlserver
    sqlserver:
      username: ${SQL_USERNAME}
      password: ${SQL_PASSWORD}
      driver: "ODBC Driver 18 for SQL Server"
      server: TEHAUSTELSQL1
      trusted_connection: false
      database: Parts_Counter_Production
      schema: dbo
      table_name: fact_weekly_production
  
  # Gold layer - Daily State Hours
  FACT_STATE_HOURS_DAILY_SQLSERVER_OUTPUT:
    target: sqlserver
    sqlserver:
      username: ${SQL_USERNAME}
      password: ${SQL_PASSWORD}
      driver: "ODBC Driver 18 for SQL Server"
      server: TEHAUSTELSQL1
      trusted_connection: false
      database: Parts_Counter_Production
      schema: dbo
      table_name: fact_state_hours_daily
  
  # Gold layer - Weekly State Hours
  FACT_STATE_HOURS_WEEKLY_SQLSERVER_OUTPUT:
    target: sqlserver
    sqlserver:
      username: ${SQL_USERNAME}
      password: ${SQL_PASSWORD}
      driver: "ODBC Driver 18 for SQL Server"
      server: TEHAUSTELSQL1
      trusted_connection: false
      database: Parts_Counter_Production
      schema: dbo
      table_name: fact_state_hours_weekly
