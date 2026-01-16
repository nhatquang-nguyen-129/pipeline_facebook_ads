import pandas as pd

def transform_campaign_metadata(df_input: pd.DataFrame) -> pd.DataFrame:

    if df_input.empty:
        return df_input.copy()

    df_output = df_input.copy()

    if "campaign_name" not in df_output.columns:
        return df_output

    df_output["platform"] = "Facebook"

    df_output = df_output.assign(
        objective=lambda df: df["adset_name"].fillna("").str.split("_").str[0].fillna("unknown"),
        budget_group=lambda df: df["adset_name"].fillna("").str.split("_").str[1].fillna("unknown"),
        region=lambda df: df["adset_name"].fillna("").str.split("_").str[2].fillna("unknown"),
        category_level_1=lambda df: df["adset_name"].fillna("").str.split("_").str[3].fillna("unknown"),

        track_group=lambda df: df["adset_name"].fillna("").str.split("_").str[6].fillna("unknown"),
        pillar_group=lambda df: df["adset_name"].fillna("").str.split("_").str[7].fillna("unknown"),
        content_group=lambda df: df["adset_name"].fillna("").str.split("_").str[8].fillna("unknown"),
     
        date=lambda df: pd.to_datetime(df["date_start"], errors="coerce", utc=True).dt.floor("D"),
        year=lambda df: pd.to_datetime(df["date_start"], errors="coerce", utc=True).dt.strftime("%Y"),
        month=lambda df: pd.to_datetime(df["date_start"], errors="coerce", utc=True).dt.strftime("%Y-%m"),
    ).drop(columns=["date_start", "date_stop"], errors="ignore")

    return df_output