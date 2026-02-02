import pandas as pd
import numpy as np


def _count_configured_processes(val) -> int:
    """
    Count how many configured process nodes are present in a CONFIGURED_PROCESSES cell.

    Parameters
    ----------
    val : Any
        Cell value that may be a delimited string like "1276, 1278" or "1276;1278".

    Returns
    -------
    int
        Number of distinct non-empty tokens detected.
    """
    if pd.isna(val):
        return 0

    s = str(val).strip()
    if not s or s.lower() in {"nan", "none"}:
        return 0

    # split on common delimiters: comma, semicolon, pipe
    tokens = [t.strip() for t in re.split(r"[;,|]", s) if t.strip()]
    # optional: de-dupe tokens
    return len(set(tokens))


def select_best_ceid_rows(
    ceid_df: pd.DataFrame,
    *,
    fab_col: str = "FAB",
    entity_col: str = "ENTITY",
    ww_col: str = "WW",
    vfmfgid_col: str = "VFMFGID",
    configured_processes_col: str = "CONFIGURED_PROCESSES",
    inserted_at_col: str | None = "InsertedAt",
) -> pd.DataFrame:
    """
    Collapse CEID_ES to one "best" row per (FAB, ENTITY) using the most recent WW and
    deterministic tie-breakers for duplicates within that WW.

    The selection logic:
    1) Keep only rows from the max WW per (FAB, ENTITY).
    2) If duplicates remain within that WW, rank rows by:
       a) ENTITY has '_PM' (preferred) over '_PC'
       b) VFMFGID is non-empty
       c) larger count of CONFIGURED_PROCESSES tokens
       d) most recent InsertedAt (if provided), else stable fallback

    Parameters
    ----------
    ceid_df : pandas.DataFrame
        Raw CEID_ES dataframe.
    fab_col : str, default "FAB"
        Column name representing fab (or fab location key).
    entity_col : str, default "ENTITY"
        Column name for tool entity.
    ww_col : str, default "WW"
        Column name for work week.
    vfmfgid_col : str, default "VFMFGID"
        Column name for VFMFGID.
    configured_processes_col : str, default "CONFIGURED_PROCESSES"
        Column name for configured processes list/string.
    inserted_at_col : str or None, default "InsertedAt"
        Column used as a "most recent row" tie-breaker. If None or missing, a stable
        fallback ordering is used.

    Returns
    -------
    pandas.DataFrame
        A filtered dataframe containing exactly one row per (FAB, ENTITY) (from the
        latest WW for that pair).

    Raises
    ------
    KeyError
        If required columns are missing.
    """
    required = {fab_col, entity_col, ww_col}
    missing = required - set(ceid_df.columns)
    if missing:
        raise KeyError(f"CEID_ES is missing required columns: {sorted(missing)}")

    df = ceid_df.copy()

    # ---- 1) Keep only latest WW per (FAB, ENTITY)
    # Note: this assumes WW is sortable (e.g., '2025WW49'). If not, normalize first.
    df["_max_ww"] = df.groupby([fab_col, entity_col])[ww_col].transform("max")
    df = df[df[ww_col] == df["_max_ww"]].drop(columns=["_max_ww"])

    # ---- 2) Build ranking features for tie-breaks
    entity_s = df[entity_col].astype(str)

    df["_pm_pref"] = entity_s.str.contains(r"_PM\d+", regex=True, na=False).astype(int)
    df["_pc_flag"] = entity_s.str.contains(r"_PC\d+", regex=True, na=False).astype(int)
    # score PM above PC; if neither, 0
    df["_pm_over_pc"] = np.where(df["_pm_pref"] == 1, 2, np.where(df["_pc_flag"] == 1, 1, 0))

    if vfmfgid_col in df.columns:
        v = df[vfmfgid_col]
        df["_has_vfmfgid"] = (
            v.notna() & (v.astype(str).str.strip() != "") & (~v.astype(str).str.lower().isin(["nan", "none"]))
        ).astype(int)
    else:
        df["_has_vfmfgid"] = 0

    if configured_processes_col in df.columns:
        import re
        df["_proc_count"] = df[configured_processes_col].apply(_count_configured_processes)
    else:
        df["_proc_count"] = 0

    # InsertedAt tie-breaker (optional)
    if inserted_at_col and inserted_at_col in df.columns:
        df["_inserted_at"] = pd.to_datetime(df[inserted_at_col], errors="coerce")
    else:
        df["_inserted_at"] = pd.NaT

    # ---- 3) Sort and take the top row per (FAB, ENTITY)
    # Higher is better for _pm_over_pc, _has_vfmfgid, _proc_count, _inserted_at
    df = df.sort_values(
        by=[fab_col, entity_col, "_pm_over_pc", "_has_vfmfgid", "_proc_count", "_inserted_at"],
        ascending=[True, True, False, False, False, False],
        kind="mergesort",  # stable sort helps reproducibility
    )

    df = df.drop_duplicates(subset=[fab_col, entity_col], keep="first")

    # cleanup
    df = df.drop(columns=["_pm_pref", "_pc_flag", "_pm_over_pc", "_has_vfmfgid", "_proc_count", "_inserted_at"], errors="ignore")

    return df









