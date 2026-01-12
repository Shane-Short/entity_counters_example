def parse_day_shift_with_year(self, day_shift: str, year: str) -> pd.Timestamp:
        """
        Parse DAY_SHIFT using year from WW column.
        
        Parameters
        ----------
        day_shift : str
            Format: "MM/DD-SX" (e.g., "12/13-S6")
        year : str
            Year from WW column (e.g., "2025")
        
        Returns
        -------
        pd.Timestamp
            Parsed date
        """
        try:
            # Extract MM/DD from DAY_SHIFT (before the " - ")
            date_part = day_shift.split(" - ")[0] if " - " in day_shift else day_shift.split("-")[0]
            
            # Combine with year from WW
            date_str = f"{year}/{date_part}"
            
            return pd.to_datetime(date_str).date()
        except Exception as e:
            logger.warning(f"Could not parse day_shift='{day_shift}' with year='{year}': {e}")
            return None
