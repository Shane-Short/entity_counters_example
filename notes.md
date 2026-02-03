def apply_altair_flag(df: pd.DataFrame, altair_path: str = "data/altair_tools.csv") -> pd.DataFrame:
    """
    Adds Altair_Flag column to a dataframe based on ENTITY matching.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe containing an ENTITY column to match against altair tools.
    altair_path : str, optional
        Path to the altair_tools.csv file. Default is "data/altair_tools.csv".

    Returns
    -------
    pd.DataFrame
        Dataframe with Altair_Flag column added (ALTAIR, MIX, or NON-ALTAIR).
    """
    # Load altair tools reference file
    altair_df = pd.read_csv(altair_path)
    altair_df["ENTITY"] = altair_df["ENTITY"].astype(str).str.strip()

    # Merge altair flag onto df based on ENTITY
    df = df.merge(
        altair_df[["ENTITY", "ProcessAllowed"]],
        on="ENTITY",
        how="left"
    )

    # Rename and fill missing/blank values with NON-ALTAIR
    df = df.rename(columns={"ProcessAllowed": "Altair_Flag"})
    df["Altair_Flag"] = df["Altair_Flag"].fillna("").str.strip()
    df.loc[df["Altair_Flag"] == "", "Altair_Flag"] = "NON-ALTAIR"

    return df
