def discover_files(self, mode: str = "full") -> List[Tuple[str, Path]]:
    """
    Discover EntityStates.csv files to process.
    """
    from utils.helpers import get_latest_ingested_data, compare_work_weeks
    
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
            latest_data = get_latest_ingested_data(
                config=self.config,
                table_name="entity_states_raw",
                schema="dbo",
                date_column=None,
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
        # INCREMENTAL MODE: Skip weeks already ingested
        # =========================================================================
        if mode == "incremental" and latest_ingested_ww:
            comparison = compare_work_weeks(ww_str, latest_ingested_ww)
            
            if comparison <= 0:
                msg = f"  Skipping {ww_str}... already ingested"
                print(msg)
                self.logger.debug(msg)
                continue
        # =========================================================================

        if file_path.exists():
            files_to_process.append((ww_str, file_path))










