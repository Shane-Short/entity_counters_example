def _create_ceid_kpis(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create tool-level KPIs aggregated by CEID + YEARWW.

        One row per CEID per YEARWW.

        Args:
            df: Enriched DataFrame

        Returns:
            DataFrame with CEID-level KPIs
        """
        self.logger.info("Creating CEID-level KPIs...")
        
        # Use named aggregations for cleaner column names
        ceid_kpis = (
            df.groupby(["FACILITY", "CEID", "YEARWW", "Location"])
            .agg(
                total_pm_events=("ENTITY", "count"),
                avg_pm_life=("CUSTOM_DELTA", "mean"),
                median_pm_life=("CUSTOM_DELTA", "median"),
                pm_life_std_dev=("CUSTOM_DELTA", "std"),
                total_downtime_hours=("DOWN_WINDOW_DURATION_HR", "sum"),
                avg_downtime_hours=("DOWN_WINDOW_DURATION_HR", "mean"),
                median_downtime_hours=("DOWN_WINDOW_DURATION_HR", "median"),
                scheduled_pm_count=("scheduled_flag", lambda x: (x == 1).sum()),
                early_pm_count=("pm_timing_classification", lambda x: (x == "Early").sum()),
                on_time_pm_count=("pm_timing_classification", lambda x: (x == "On-Time").sum()),
                late_pm_count=("pm_timing_classification", lambda x: (x == "Late").sum()),
                overdue_pm_count=("pm_timing_classification", lambda x: (x == "Overdue").sum()),
                reclean_count=("reclean_event_flag", lambda x: (x == 1).sum()),
                sympathy_pm_count=("sympathy_pm_flag", lambda x: (x == 1).sum()),
                ww_year=("ww_year", "first"),
                ww_number=("ww_number", "first"),
            )
            .reset_index()
        )
        
        self.logger.info(f"CEID KPIs columns after aggregation: {list(ceid_kpis.columns)}")

        # Calculate derived metrics
        ceid_kpis["unscheduled_pm_count"] = (
            ceid_kpis["total_pm_events"] - ceid_kpis["scheduled_pm_count"]
        )
        
        ceid_kpis["unscheduled_pm_rate"] = (
            ceid_kpis["unscheduled_pm_count"] / ceid_kpis["total_pm_events"]
        ).fillna(0)

        ceid_kpis["reclean_rate"] = (
            ceid_kpis["reclean_count"] / ceid_kpis["total_pm_events"]
        ).fillna(0)

        ceid_kpis["calculation_timestamp"] = datetime.now()
        
        self.logger.info(f"Created {len(ceid_kpis):,} CEID KPI rows")

        return ceid_kpis
