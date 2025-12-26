# Find files for each work week
        files_to_process = []
        
        # Get search range once (up to 4 weeks back)
        search_weeks = get_recent_work_weeks(4)
        
        for ww_str in work_weeks:
            file_found = False
            
            # Search backwards through available weeks
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






# Find latest Counters file for each work week
        files_to_process = []
        
        # Get search range once (up to 4 weeks back)
        search_weeks = get_recent_work_weeks(4)
        
        for ww_str in work_weeks:
            file_found = False
            
            # Search backwards through available weeks
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





