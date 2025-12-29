def calculate_wafer_production_single_row(self, 
                                             current_row: pd.Series,
                                             previous_row: Optional[pd.Series],
                                             running_hours: float) -> Dict:
        """
        Calculate wafer production for a single entity-date row.
        
        Logic:
        1. Find first counter > 1000 in CURRENT row
        2. Use THAT SAME counter to compare with previous row
        3. Calculate change using same counter column for both days
        
        Parameters
        ----------
        current_row : pd.Series
            Current day row with counter data
        previous_row : pd.Series or None
            Previous day row
        running_hours : float
            Running state hours for this day
        
        Returns
        -------
        Dict
            Dictionary with production metrics
        """
        entity = current_row['ENTITY']
        date = str(current_row['counter_date'])
        
        result = {
            'ENTITY': entity,
            'counter_date': current_row['counter_date'],
            'counter_column_used': None,
            'counter_keyword_used': 'auto',
            'counter_current_value': None,
            'counter_previous_value': None,
            'counter_change': None,
            'part_replacement_detected': False,
            'wafers_produced': None,
            'running_hours': running_hours,
            'wafers_per_hour': None,
            'calculation_notes': []
        }
        
        # STEP 1: Find first counter > 1000 in CURRENT row
        counter_found = self.find_counter_column(current_row)
        
        if not counter_found:
            result['calculation_notes'].append('No counter found with value > 1000')
            return result
        
        counter_col, current_val, _ = counter_found
        result['counter_column_used'] = counter_col
        result['counter_current_value'] = current_val
        
        logger.debug(f"{entity} ({date}): Using counter '{counter_col}' (value: {current_val})")
        
        # STEP 2: Check if we have previous row
        if previous_row is None:
            result['calculation_notes'].append('First day - no previous value')
            return result
        
        # STEP 3: Get value from SAME counter column in previous row
        if counter_col not in previous_row.index:
            result['calculation_notes'].append(f'Counter {counter_col} not in previous row')
            return result
        
        previous_val = previous_row[counter_col]
        if pd.isna(previous_val):
            result['calculation_notes'].append(f'Previous value for {counter_col} is null')
            return result
        
        result['counter_previous_value'] = previous_val
        change = current_val - previous_val
        result['counter_change'] = change
        
        # STEP 4: Check for negative change (part replacement)
        if change < 0:
            logger.debug(f"{entity} ({date}): Negative change detected: {counter_col} went from {previous_val} to {current_val} (change: {change})")
            
            # Check if it's a part replacement (threshold: -1000)
            if change < self.replacement_threshold:
                result['part_replacement_detected'] = True
                result['calculation_notes'].append(f'Part replacement: {counter_col} reset from {previous_val} to {current_val}')
                logger.info(f"PART REPLACEMENT - {entity} ({date}): {counter_col} dropped {change}")
                
                # Set change to 0 for part replacement
                result['counter_change'] = 0
                result['calculation_notes'].append('Counter change set to 0 (part replacement)')
            else:
                # Small negative change - might be data error, set to 0
                result['counter_change'] = 0
                result['calculation_notes'].append(f'Small negative change ({change}) - set to 0')
        
        # STEP 5: Calculate wafers produced and wafers per hour
        if result['counter_change'] is not None and result['counter_change'] >= 0:
            result['wafers_produced'] = result['counter_change']
            
            if running_hours > 0:
                result['wafers_per_hour'] = result['wafers_produced'] / running_hours
            else:
                result['calculation_notes'].append('No running hours - cannot calculate wafers/hour')
        
        return result


