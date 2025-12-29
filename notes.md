def classify_state(self, state: str) -> str:
        """
        Classify entity state into category.
        
        Categories:
        - running: Running1-8
        - idle: UpToProduction states
        - bagged: Exact 'Bagged' match
        - down: Everything else (WaitingTechnician, UnschWaitSupplier, etc.)
        
        Note: All states are now tracked individually as well
        
        Parameters
        ----------
        state : str
            Entity state string
        
        Returns
        -------
        str
            State category
        """
        if not state or pd.isna(state):
            return 'down'
        
        state_clean = str(state).strip()
        
        # Running states
        running_states = self.config.get('running_states', [
            'Running1', 'Running2', 'Running3', 'Running4',
            'Running5', 'Running6', 'Running7', 'Running8'
        ])
        if state_clean in running_states:
            return 'running'
        
        # Idle states (UpToProduction)
        if 'UpToProduction' in state_clean:
            return 'idle'
        
        # Bagged
        if state_clean == 'Bagged':
            return 'bagged'
        
        # Everything else is "down" (no longer log as unknown)
        return 'down'









def aggregate_by_state(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate hours by entity, date, and individual state.
        Creates a detailed breakdown of all states.
        
        Parameters
        ----------
        df : pd.DataFrame
            EntityStates data
        
        Returns
        -------
        pd.DataFrame
            Aggregated by entity, date, and each unique state
        """
        # Group by ENTITY, state_date, and actual ENTITY_STATE
        state_detail = df.groupby(['ENTITY', 'state_date', 'ENTITY_STATE'])['HOURS_IN_STATE'].sum().reset_index()
        state_detail = state_detail.rename(columns={'HOURS_IN_STATE': 'hours', 'ENTITY_STATE': 'state_name'})
        
        logger.info(f"Detailed state tracking: {len(state_detail)} entity-date-state combinations")
        logger.info(f"Unique states found: {state_detail['state_name'].nunique()}")
        
        return state_detail




# Group by entity
        for entity, entity_group in counters_df.groupby('ENTITY'):
            entity_group = entity_group.sort_values('counter_date').reset_index(drop=True)
            
            # Skip if only 1 day of data - can't calculate production without previous day
            if len(entity_group) == 1:
                logger.debug(f"Skipping {entity}: only 1 day of data, no previous comparison possible")
                # Still create a result row but with no wafer calculation
                result = {
                    'ENTITY': entity,
                    'counter_date': entity_group.iloc[0]['counter_date'],
                    'counter_column_used': None,
                    'counter_keyword_used': None,
                    'counter_current_value': None,
                    'counter_previous_value': None,
                    'counter_change': None,
                    'part_replacement_detected': False,
                    'wafers_produced': None,
                    'running_hours': 0,
                    'wafers_per_hour': None,
                    'calculation_notes': ['Only 1 day of data - no previous comparison']
                }
                results.append(result)
                continue
            
            # Process each day
            for idx, current_row in entity_group.iterrows():
                # Get previous row (only within same entity)
                previous_row = entity_group.iloc[idx - 1] if idx > 0 else None
