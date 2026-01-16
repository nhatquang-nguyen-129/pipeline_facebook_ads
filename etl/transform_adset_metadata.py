import pandas as pd

def transform_adset_metadata(df_input: pd.DataFrame) -> pd.DataFrame:

    if df_input.empty:
        return df_input.copy()

    df_output = df_input.copy()

    if "adset_name" not in df_output.columns:
        return df_output

    df_output = df_output.assign(
        location=lambda df: df["adset_name"].fillna("").str.split("_").str[0].fillna("unknown"),
        gender=lambda df: df["adset_name"].fillna("").str.split("_").str[1].fillna("unknown"),
        age=lambda df: df["adset_name"].fillna("").str.split("_").str[2].fillna("unknown"),
        audience=lambda df: df["adset_name"].fillna("").str.split("_").str[3].fillna("unknown"),
        format=lambda df: df["adset_name"].fillna("").str.split("_").str[4].fillna("unknown"),
        strategy=lambda df: df["adset_name"].fillna("").str.split("_").str[5].fillna("unknown"),
        subtype=lambda df: df["adset_name"].fillna("").str.split("_").str[6].fillna("unknown"),
        pillar=lambda df: df["adset_name"].fillna("").str.split("_").str[7].fillna("unknown"),
        content=lambda df: df["adset_name"].fillna("").str.split("_").str[8].fillna("unknown")
    )  

    return df_output