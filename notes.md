def discover_files(self, mode: str = "full") -> List[Tuple[str, Path]]:
        """
        Discover EntityStates.csv files to process.
        Searches backwards through work weeks until finding the requested number of files.
        
        Parameters
        ----------
        mode : str
            'full' or 'incremental'
        
        Returns
        -------
        List[Tuple[str, Path]]
            List of (work_week_string, file_path) tuples
        """
        
        logger.info("Starting file discovery")
        
        root_path = Path(self.source_config["root_path"])
        file_name = self.entity_states_config["file_name"]
        
        if mode == "full":
            # Load last N weeks of AVAILABLE data
            num_weeks = self.historical_config["weeks_to_load"]
            self.logger.info(f"Full refresh mode: Looking for {num_weeks} most recent available files")
        else:
            # Load current week only
            num_weeks = 1
            self.logger.info("Incremental mode: Looking for most recent available file")
        
        files_to_process = []
        
        # Search backwards through work weeks until we find enough files
        # Start from current week and go back up to 52 weeks
        search_limit = 52
        all_work_weeks = get_recent_work_weeks(search_limit)
        
        logger.info(f"Searching for {num_weeks} most recent EntityStates files")
        logger.info(f"Will search backwards through up to {search_limit} weeks")
        
        for ww_str in all_work_weeks:
            if len(files_to_process) >= num_weeks:
                break  # Found enough files
            
            ww_folder = root_path / ww_str
            file_path = ww_folder / file_name
            
            if file_path.exists():
                files_to_process.append((ww_str, file_path))
                self.logger.info(f"Found EntityStates file for {ww_str}: {file_path} ({len(files_to_process)}/{num_weeks})")
            else:
                logger.debug(f"No file in {ww_str}, continuing search")
        
        if len(files_to_process) < num_weeks:
            self.logger.warning(
                f"Only found {len(files_to_process)}/{num_weeks} EntityStates files after searching {search_limit} weeks"
            )
        
        logger.info(f"Discovery complete: found {len(files_to_process)} files")
        self.logger.info(f"Discovered {len(files_to_process)} EntityStates files to process")
        
        return files_to_process









def discover_files(self, mode: str = "full") -> List[Tuple[str, Path, datetime]]:
        """
        Discover Counters .csv files to process.
        
        For each work week found, retrieves the 7 most recent Counters files by modified date.
        Searches backwards through work weeks until finding the requested number of weeks with files.
        
        Parameters
        ----------
        mode : str
            'full' or 'incremental'
        
        Returns
        -------
        List[Tuple[str, Path, datetime]]
            List of (work_week_string, file_path, modified_datetime) tuples
        """
        
        root_path = Path(self.source_config['root_path'])
        file_prefix = self.counters_config['file_prefix']
        files_per_week = self.counters_config.get('files_per_week', 7)  # Default to 7
        
        if mode == "full":
            # Load last N weeks of AVAILABLE data
            num_weeks = self.historical_config['weeks_to_load']
            self.logger.info(f"Full refresh mode: Looking for {num_weeks} most recent available weeks")
        else:
            # Load current week only
            num_weeks = 1
            self.logger.info("Incremental mode: Looking for most recent available week")
        
        files_to_process = []
        weeks_found = 0
        
        # Search backwards through work weeks until we find enough weeks with files
        search_limit = 52
        all_work_weeks = get_recent_work_weeks(search_limit)
        
        self.logger.info(f"Searching for {num_weeks} most recent weeks with Counters files")
        self.logger.info(f"Will search backwards through up to {search_limit} weeks")
        
        for ww_str in all_work_weeks:
            if weeks_found >= num_weeks:
                break  # Found enough weeks
            
            ww_folder = root_path / ww_str
            
            if not ww_folder.exists():
                logger.debug(f"Folder does not exist: {ww_folder}, continuing search")
                continue
            
            # Find all Counters files in this week's folder
            counter_files = list(ww_folder.glob(f"{file_prefix}*.csv"))
            
            if not counter_files:
                logger.debug(f"No Counters files in {ww_str}, continuing search")
                continue
            
            # Get modified times and sort by most recent first
            files_with_times = []
            for file_path in counter_files:
                try:
                    modified_timestamp = file_path.stat().st_mtime
                    modified_dt = datetime.fromtimestamp(modified_timestamp, tz=timezone.utc)
                    files_with_times.append((file_path, modified_dt))
                except Exception as e:
                    logger.warning(f"Could not get modified time for {file_path}: {e}")
                    continue
            
            # Sort by modified date (most recent first) and take the top N files
            files_with_times.sort(key=lambda x: x[1], reverse=True)
            files_to_take = files_with_times[:files_per_week]
            
            # Add to results
            for file_path, modified_dt in files_to_take:
                files_to_process.append((ww_str, file_path, modified_dt))
            
            weeks_found += 1
            self.logger.info(
                f"Found {len(files_to_take)} Counters files for {ww_str} "
                f"(newest: {files_to_take[0][0].name}) "
                f"({weeks_found}/{num_weeks} weeks)"
            )
        
        if weeks_found < num_weeks:
            self.logger.warning(
                f"Only found {weeks_found}/{num_weeks} weeks with Counters files after searching {search_limit} weeks"
            )
        
        self.logger.info(
            f"Discovered {len(files_to_process)} total Counters files from {weeks_found} weeks"
        )
        
        return files_to_process










