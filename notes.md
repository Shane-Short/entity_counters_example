def find_counter_column(self, row: pd.Series, keywords: List[str] = None) -> Optional[Tuple[str, float, str]]:
        """
        Find first counter column with value > 1000.
        Simple approach: use first valid counter regardless of keywords.
        
        Parameters
        ----------
        row : pd.Series
            DataFrame row
        keywords : List[str], optional
            Not used - kept for compatibility
        
        Returns
        -------
        Tuple[str, float, str] or None
            (column_name, value, 'auto') if found, None otherwise
        """
        # Get all counter columns
        counter_cols = [col for col in row.index if col.endswith('Counter')]
        
        # Find first counter with value > 1000
        for col in counter_cols:
            value = row[col]
            if pd.notna(value) and value > 1000:
                return (col, value, 'auto')
        
        return None







if not counter_found:
            result['calculation_notes'].append('No counter found with value > 1000')
            return result







            
