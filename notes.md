import pandas as pd
import re
from utils.logger import get_logger

log = get_logger(__name__)


def normalize_customer_tool_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalizes the CUSTOMER_TOOL_ID column by appending WISDOM_INSTALL_POSITION
    when the tool ID doesn't already contain an underscore (i.e., it's a MOM-level ID
    that needs a chamber/position suffix).

    This function should be called IMMEDIATELY after extraction and BEFORE any other
    transformations to ensure all rows have properly formatted tool IDs.

    Logic:
    - If CUSTOMER_TOOL_ID already contains "_", leave it unchanged
    - If CUSTOMER_TOOL_ID does NOT contain "_" AND WISDOM_INSTALL_POSITION is not null/blank:
        - If position is like "P1", "P2", etc. → append as "_PM1", "_PM2" (add the 'M')
        - If position is like "PM1", "PM2", etc. → append directly as "_PM1", "_PM2"
        - If position is "VTM" → append as "_VTM"
        - If position is "LM" → append as "_LM"
        - Other non-null positions → append directly with underscore

    Parameters
    ----------
    df : pd.DataFrame
        Raw WISDOM_GLOBAL_MACHINES DataFrame with at least columns:
        - CUSTOMER_TOOL_ID
        - WISDOM_INSTALL_POSITION

    Returns
    -------
    pd.DataFrame
        DataFrame with normalized CUSTOMER_TOOL_ID values.
    """
    if df is None or df.empty:
        log.warning("[NORMALIZE] DataFrame is None or empty. Skipping normalization.")
        return df

    # Validate required columns exist
    required_cols = ["CUSTOMER_TOOL_ID", "WISDOM_INSTALL_POSITION"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        log.error(f"[NORMALIZE ERROR] Missing required columns: {missing_cols}")
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Create a copy to avoid modifying the original
    df = df.copy()

    # Track changes for logging
    original_ids = df["CUSTOMER_TOOL_ID"].copy()

    def build_normalized_id(row):
        """
        Builds the normalized CUSTOMER_TOOL_ID for a single row.
        """
        tool_id = row["CUSTOMER_TOOL_ID"]
        position = row["WISDOM_INSTALL_POSITION"]

        # If tool_id is null/blank, return as-is
        if pd.isna(tool_id) or str(tool_id).strip() == "":
            return tool_id

        tool_id_str = str(tool_id).strip()

        # If already contains underscore, no modification needed
        if "_" in tool_id_str:
            return tool_id_str

        # If position is null/blank, return tool_id unchanged
        if pd.isna(position) or str(position).strip() == "":
            return tool_id_str

        position_str = str(position).strip().upper()

        # Pattern: "P" followed by one or more digits (e.g., P1, P2, P10)
        # These need "M" added to become PM1, PM2, PM10
        p_digit_pattern = re.match(r"^P(\d+)$", position_str)
        if p_digit_pattern:
            digit = p_digit_pattern.group(1)
            return f"{tool_id_str}_PM{digit}"

        # Pattern: Already "PM" followed by digits (e.g., PM1, PM2)
        # Append directly
        pm_digit_pattern = re.match(r"^PM\d+$", position_str)
        if pm_digit_pattern:
            return f"{tool_id_str}_{position_str}"

        # Special cases: VTM, LM - append directly
        if position_str in ["VTM", "LM"]:
            return f"{tool_id_str}_{position_str}"

        # For any other non-empty position value, append with underscore
        # This handles edge cases we might not have anticipated
        return f"{tool_id_str}_{position_str}"

    # Apply normalization
    df["CUSTOMER_TOOL_ID"] = df.apply(build_normalized_id, axis=1)

    # Log statistics
    changed_mask = df["CUSTOMER_TOOL_ID"] != original_ids
    num_changed = changed_mask.sum()
    total_rows = len(df)

    log.info(
        f"[NORMALIZE] Normalized CUSTOMER_TOOL_ID: {num_changed} of {total_rows} rows modified."
    )

    # Log sample of changes for debugging (first 5)
    if num_changed > 0:
        sample_changes = df.loc[changed_mask, ["CUSTOMER_TOOL_ID"]].head(5)
        original_sample = original_ids.loc[changed_mask].head(5)
        log.debug(f"[NORMALIZE] Sample changes:")
        for idx in sample_changes.index[:5]:
            log.debug(
                f"  {original_ids.loc[idx]} -> {df.loc[idx, 'CUSTOMER_TOOL_ID']}"
            )

    return df
