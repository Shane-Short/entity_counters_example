def discover_files(self, mode: str = 'full') -> List[Tuple[str, Path]]:
        """
        Discover EntityStates.csv files to process.
        Searches backwards up to 4 weeks if file not found in expected week.
        
        Parameters
        ----------
        mode : str
            'full' or 'incremental'
        
        Returns
        -------
        List[Tuple[str, Path]]
            List of (work_week_string, file_path) tuples
        """
        root_path = Path(self.source_config['root_path'])
        file_name = self.entity_states_config['file_name']
        
        if mode == 'full':
            # Load last N weeks
            num_weeks = self.historical_config['weeks_to_load']
            work_weeks = get_recent_work_weeks(num_weeks)
            self.logger.info(f"Full refresh mode: Loading {num_weeks} weeks of data")
        else:
            # Load current week only
            work_weeks = get_recent_work_weeks(1)
            self.logger.info(f"Incremental mode: Loading current week only")
        
        # Find files for each work week
        files_to_process = []
        for ww_str in work_weeks:
            file_found = False
            
            # Search backwards up to 4 weeks if file not found
            search_weeks = get_recent_work_weeks(4)
            
            for search_ww in search_weeks:
                ww_folder = root_path / search_ww
                file_path = ww_folder / file_name
                
                if file_path.exists():
                    files_to_process.append((ww_str, file_path))
                    if search_ww != ww_str:
                        self.logger.info(f"EntityStates file for {ww_str} found in {search_ww}: {file_path}")
                    else:
                        self.logger.info(f"Found EntityStates file for {ww_str}: {file_path}")
                    file_found = True
                    break
            
            if not file_found:
                self.logger.warning(f"EntityStates file not found for {ww_str} (searched back 4 weeks)")
        
        self.logger.info(f"Discovered {len(files_to_process)} EntityStates files to process")
        return files_to_process







        def discover_files(self, mode: str = 'full') -> List[Tuple[str, Path, datetime]]:
        """
        Discover Counters_*.csv files to process.
        Finds the LATEST file in each work week by modified date.
        Searches backwards up to 4 weeks if file not found in expected week.
        
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
        
        if mode == 'full':
            # Load last N weeks
            num_weeks = self.historical_config['weeks_to_load']
            work_weeks = get_recent_work_weeks(num_weeks)
            self.logger.info(f"Full refresh mode: Loading {num_weeks} weeks of data")
        else:
            # Load current week only
            work_weeks = get_recent_work_weeks(1)
            self.logger.info(f"Incremental mode: Loading current week only")
        
        # Find latest Counters file for each work week
        files_to_process = []
        for ww_str in work_weeks:
            file_found = False
            
            # Search backwards up to 4 weeks if file not found
            search_weeks = get_recent_work_weeks(4)
            
            for search_ww in search_weeks:
                result = find_latest_counters_file(root_path, search_ww, file_prefix)
                if result:
                    file_path, modified_dt = result
                    files_to_process.append((ww_str, file_path, modified_dt))
                    if search_ww != ww_str:
                        self.logger.info(f"Counters file for {ww_str} found in {search_ww}: {file_path.name}")
                    file_found = True
                    break
            
            if not file_found:
                self.logger.warning(f"No Counters file found for {ww_str} (searched back 4 weeks)")
        
        self.logger.info(f"Discovered {len(files_to_process)} Counters files to process")
        return files_to_process
