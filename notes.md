def get_latest_ingested_data(
    config: Dict,
    table_name: str,
    schema: str = "dbo",
    date_column: str = None,
    ww_column: str = "load_ww"
) -> dict:
    """
    Get the latest ingested data point from a Bronze table.
    
    Parameters
    ----------
    config : Dict
        Configuration dictionary
    table_name : str
        Name of the Bronze table (e.g., 'counters_raw', 'entity_states_raw')
    schema : str
        Database schema (default: 'dbo')
    date_column : str, optional
        Name of the date column to check (e.g., 'counter_date')
    ww_column : str
        Name of the work week column (default: 'load_ww')
        
    Returns
    -------
    dict
        {
            'max_load_ww': str or None,
            'max_date': date or None,
            'row_count': int
        }
    """
    from utils.database_engine import get_database_connection
    import logging
    
    logger = logging.getLogger(__name__)
    
    result = {
        'max_load_ww': None,
        'max_date': None,
        'row_count': 0
    }
    
    try:
        conn = get_database_connection(config)
        cursor = conn.cursor()
        
        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}")
        result['row_count'] = cursor.fetchone()[0] or 0
        
        if result['row_count'] == 0:
            logger.info(f"Table {table_name} is empty - no previous ingestion found")
            cursor.close()
            conn.close()
            return result
        
        # Get max load_ww
        cursor.execute(f"SELECT MAX({ww_column}) FROM {schema}.{table_name}")
        result['max_load_ww'] = cursor.fetchone()[0]
        
        # Get max date if date column specified
        if date_column:
            cursor.execute(f"SELECT MAX({date_column}) FROM {schema}.{table_name}")
            result['max_date'] = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        logger.info(
            f"Latest ingested data in {table_name}: "
            f"max_load_ww={result['max_load_ww']}, "
            f"max_date={result['max_date']}, "
            f"row_count={result['row_count']}"
        )
        
    except Exception as e:
        logger.warning(f"Could not query {table_name} for latest data: {e}")
        logger.warning("Assuming table is empty or doesn't exist yet")
    
    return result


def compare_work_weeks(ww1: str, ww2: str) -> int:
    """
    Compare two work week strings.
    
    Returns
    -------
    int
        -1 if ww1 < ww2, 0 if equal, 1 if ww1 > ww2
    """
    try:
        year1 = int(ww1[:4])
        week1 = int(ww1[-2:])
        year2 = int(ww2[:4])
        week2 = int(ww2[-2:])
        
        if year1 != year2:
            return -1 if year1 < year2 else 1
        if week1 != week2:
            return -1 if week1 < week2 else 1
        return 0
    except (ValueError, IndexError, TypeError):
        if ww1 < ww2:
            return -1
        elif ww1 > ww2:
            return 1
        return 0












def discover_files(
    self, mode: str = "full"
) -> List[Tuple[str, Path, datetime]]:
    """
    Discover Counters.csv files to process.
    """
    from utils.helpers import get_latest_ingested_data, compare_work_weeks
    
    root_path = Path(self.source_config["root_path"])
    file_prefix = self.counters_config["file_prefix"]
    files_per_week = self.counters_config.get("files_per_week", 7)

    # =========================================================================
    # INCREMENTAL MODE: Get latest ingested data to avoid duplicates
    # =========================================================================
    latest_ingested_ww = None
    latest_ingested_date = None
    
    if mode == "incremental":
        self.logger.info("Incremental mode: Checking for previously ingested data...")
        print("Incremental mode: Checking for previously ingested data...")
        
        try:
            latest_data = get_latest_ingested_data(
                config=self.config,
                table_name="counters_raw",
                schema="dbo",
                date_column="counter_date",
                ww_column="load_ww"
            )
            
            latest_ingested_ww = latest_data['max_load_ww']
            latest_ingested_date = latest_data['max_date']
            
            if latest_ingested_ww:
                self.logger.info(
                    f"Latest ingested data: WW={latest_ingested_ww}, Date={latest_ingested_date}"
                )
                print(f"Latest ingested data: WW={latest_ingested_ww}, Date={latest_ingested_date}")
            else:
                self.logger.info("No previous data found - will process all available files")
                print("No previous data found - will process all available files")
                
        except Exception as e:
            self.logger.warning(f"Could not check for previous data: {e}")
            print(f"Warning: Could not check for previous data: {e}")
    # =========================================================================

    if mode == "full":
        num_weeks = self.historical_config["weeks_to_load"]
        self.logger.info(
            f"Full refresh mode: Looking for {num_weeks} most recent available weeks"
        )
    else:
        num_weeks = 52  # Search up to a year to find all new data
        self.logger.info(
            "Incremental mode: Looking for data newer than last ingestion"
        )










for ww_str in all_work_weeks:
    if weeks_found >= num_weeks:
        break

    ww_folder = root_path / ww_str

    if not ww_folder.exists():
        msg = f"  Checking {ww_str}... folder not found"
        print(msg)
        logger.debug(msg)
        continue

    # =========================================================================
    # INCREMENTAL MODE: Skip weeks we've already fully ingested
    # =========================================================================
    if mode == "incremental" and latest_ingested_ww:
        comparison = compare_work_weeks(ww_str, latest_ingested_ww)
        
        if comparison < 0:
            # This week is OLDER than what we have - skip entirely
            msg = f"  Skipping {ww_str}... already ingested (older than {latest_ingested_ww})"
            print(msg)
            self.logger.debug(msg)
            continue
        elif comparison == 0:
            # This is the SAME week - check individual files by date
            msg = f"  Checking {ww_str}... same as latest ingested week, will filter by date"
            print(msg)
            self.logger.info(msg)
            # Continue to process but we'll filter individual files below
    # =========================================================================

    counter_files = list(ww_folder.glob(f"{file_prefix}*.csv"))












for file_path, modified_dt in files_to_take:
    # =========================================================================
    # INCREMENTAL MODE: For current week, skip files older than last ingestion
    # =========================================================================
    if mode == "incremental" and latest_ingested_ww and latest_ingested_date:
        if compare_work_weeks(ww_str, latest_ingested_ww) == 0:
            # Same week - check if this file's date is newer
            # File date is modified_dt minus 1 day (counter_date adjustment)
            from datetime import timedelta
            file_date = (modified_dt - timedelta(days=1)).date()
            
            if file_date <= latest_ingested_date:
                msg = f"    Skipping {file_path.name}... already ingested (date {file_date})"
                print(msg)
                self.logger.debug(msg)
                continue
    # =========================================================================
    
    files_to_process.append((ww_str, file_path, modified_dt))







def discover_files(self, mode: str = "full") -> List[Tuple[str, Path]]:
    """
    Discover EntityStates.csv files to process.
    """
    from utils.helpers import get_latest_ingested_data, compare_work_weeks
    from utils.database_engine import get_engine
    
    logger.info("Starting file discovery")

    root_path = Path(self.source_config["root_path"])
    file_name = self.entity_states_config["file_name"]

    # =========================================================================
    # INCREMENTAL MODE: Get latest ingested data to avoid duplicates
    # =========================================================================
    latest_ingested_ww = None
    
    if mode == "incremental":
        self.logger.info("Incremental mode: Checking for previously ingested data...")
        print("Incremental mode: Checking for previously ingested data...")
        
        try:
            engine = get_engine(self.config)
            latest_data = get_latest_ingested_data(
                engine=engine,
                table_name="entity_states_raw",
                date_column=None,  # EntityStates doesn't have a date column
                ww_column="load_ww"
            )
            
            latest_ingested_ww = latest_data['max_load_ww']
            
            if latest_ingested_ww:
                self.logger.info(f"Latest ingested data: WW={latest_ingested_ww}")
                print(f"Latest ingested data: WW={latest_ingested_ww}")
            else:
                self.logger.info("No previous data found - will process all available files")
                print("No previous data found - will process all available files")
                
        except Exception as e:
            self.logger.warning(f"Could not check for previous data: {e}")
            print(f"Warning: Could not check for previous data: {e}")
    # =========================================================================

    if mode == "full":
        num_weeks = self.historical_config["weeks_to_load"]
        self.logger.info(
            f"Full refresh mode: Looking for {num_weeks} most recent available files"
        )
    else:
        # For incremental, search for ALL weeks newer than what we have
        num_weeks = 52
        self.logger.info(
            "Incremental mode: Looking for data newer than last ingestion"
        )











for ww_str in all_work_weeks:
    if len(files_to_process) >= num_weeks:
        break

    ww_folder = root_path / ww_str
    file_path = ww_folder / file_name

    # =========================================================================
    # INCREMENTAL MODE: Skip weeks we've already ingested
    # =========================================================================
    if mode == "incremental" and latest_ingested_ww:
        comparison = compare_work_weeks(ww_str, latest_ingested_ww)
        
        if comparison <= 0:
            # This week is OLDER than or SAME as what we have - skip
            msg = f"  Skipping {ww_str}... already ingested"
            print(msg)
            self.logger.debug(msg)
            continue
    # =========================================================================

    if file_path.exists():
        files_to_process.append((ww_str, file_path))
        msg = (
            f"Found EntityStates for {ww_str}: "
            f"{file_path.name} ({len(files_to_process)}/{num_weeks})"
        )
        print(msg)
        self.logger.info(msg)
    else:
        msg = f"Checking {ww_str}... not found"
        print(msg)
        logger.debug(msg)










