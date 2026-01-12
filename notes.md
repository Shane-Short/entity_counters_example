def parse_day_shift_with_year(self, day_shift: str, year: str) -> Optional[date]:
        """
        Parse DAY_SHIFT using year from WW column.
        Handles December dates in January work weeks (year-end boundary).
        """
        try:
            # Extract MM/DD from DAY_SHIFT
            date_part = day_shift.split(" - ")[0] if " - " in day_shift else day_shift.split("-")[0]
            month_str, day_str = date_part.split("/")
            month = int(month_str)
            
            # Extract WW from year string (e.g., "2026" from WW "202601")
            # Assuming year is already just "2026"
            year_int = int(year)
            
            # YEAR-BOUNDARY FIX: December dates in WW 01 belong to previous year
            # Example: 12/31 in 202601 → 2025-12-31
            if month == 12:
                # Build the date and check if it's in the future
                temp_date = pd.to_datetime(f"{year_int}/{date_part}")
                if temp_date > pd.Timestamp.now():
                    year_int -= 1
            
            # Build final date string
            date_str = f"{year_int}/{date_part}"
            return pd.to_datetime(date_str).date()
            
        except Exception as e:
            logger.warning(f"Could not parse day_shift='{day_shift}' with year='{year}': {e}")
            return None
