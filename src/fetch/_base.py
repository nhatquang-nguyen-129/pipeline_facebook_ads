import os
import sys
from abc import (
    ABC,
    abstractmethod
)
import ast
import logging
import pandas as pd
sys.path.append(
    os.path.abspath
    (os.path.join(
        os.path.dirname(__file__), "../../"
        )
    )
)

class BaseFetch(ABC):
    """
    Abstract base class for fetching data from various backends
    ---------
    Workflow:
        1. Validate fetch config    : Validates that the fetch config only contains allowed keys
        2. Retrieve raw data        : Implement the backend-specific data retrieval logic
        3. Apply optional WHERE     : Filters the DataFrame according to WHERE configuration
        4. Apply optional SELECT    : Selects or excludes columns according to SELECT configuration
        5. Apply optional AGGREGATE : Performs aggregation according to AGGREGATE configuration
        6. Apply optional ORDER BY  : Sorts the DataFrame according to ORDER BY configuration
        7. Apply optional LIMIT     : Limits the number of rows returned according to LIMIT configuration
        8. Apply optional SAMPLE    : Returns a random sample according to SAMPLE configuration
    ----------
    Parameters
        1. backend : str
        The type of backend to fetch data from (e.g., 'local', 'bigquery', 'spreadsheet').
        2. direction : str
        The source location or identifier, such as a file path, table, or spreadsheet ID.
    """

    VALID_CONFIG_KEYS = {
        "select",
        "where",
        "order_by",
        "limit",
        "aggregate",
        "sample",
    }

    SUPPORTED_CONFIG_KEYS: set[str] = set()

# 1.1. Initialize
    def __init__(
            self, 
            backend: str, 
            direction: str
        ):
        
        self.backend = backend.lower()
        self.direction = direction

# 1.2. Workflow

    # 1.2.1. Validate fetch config
    def _validate_fetch_config(
            self, 
            config: dict
        ) -> dict:
        
        if not isinstance(config, dict):
            raise TypeError("❌ [FETCH] Failed to validate due to fetch config must be a dict.")

        invalid_config_keys = set(config) - self.VALID_CONFIG_KEYS
        if invalid_config_keys:
            raise ValueError(
                f"❌ [FETCH] Failed to validate due to invalid fetch config key(s) "
                f"{invalid_config_keys} not in allowed key list "
                f"{self.VALID_CONFIG_KEYS}."
            )
        
        return config

    # 1.2.2. Fetch
    @abstractmethod
    def _fetch(self) -> pd.DataFrame:
        raise NotImplementedError

    # 1.2.3. Apply WHERE query config
    def _apply_where_config(
            self, 
            df: pd.DataFrame, 
            config: dict) -> pd.DataFrame:

        where = config.get("where")
        if not where:
            return df

        conditions = where.get("conditions")
        if not conditions:
            return df

        if not isinstance(conditions, list):
            raise ValueError("❌ [FETCH] Failed to apply WHERE config due to conditions input must be a list.")

        for idx, cond in enumerate(conditions, start=1):
            if not isinstance(cond, dict):
                raise ValueError(
                    f"❌ [FETCH] Failed to apply WHERE config due to condition "
                    f"{idx} must be a dict.")

            col = cond.get("field")
            op = cond.get("op")
            val = cond.get("value")

            if not col or not op:
                raise ValueError(
                    f"❌ [FETCH] Faield to apply WHERE config due to condition "
                    f"{idx} missing field or op.")

            if col not in df.columns:
                msg = (
                    f"⚠️ [FETCH] Failed to apply WHERE config due to column "
                    f"{col} not found in condition "
                    f"{idx} then appplication will be skipped."
                )
                print(msg)
                logging.warning(msg)
                continue

            series = df[col]

            if op in {
                "in", 
                "not in"
            }:               
                if val is None:
                    val = []                               
                if not isinstance(
                    val, (
                        list, 
                        tuple, 
                        set, pd.Series
                        )
                ):
                    val = [val]                                  
                if isinstance(val, str):
                    try:
                        val = ast.literal_eval(val)
                    except Exception:
                        raise ValueError(
                            "❌ [FETCH] Failed to apply WHERE config due to op "
                            f"{op} in condition "
                            f"{idx} requires list-like value"
                        )         
            else:
                if val is None:
                    val = None

            try:
                if pd.api.types.is_datetime64_any_dtype(series):
                    if isinstance(
                        val, (
                            list, 
                            tuple, 
                            set
                        )
                    ):
                        val = pd.to_datetime(list(val), errors="coerce")
                    else:
                        val = pd.to_datetime(val, errors="coerce")
                elif pd.api.types.is_numeric_dtype(series):
                    if isinstance(
                        val, (
                            list, 
                            tuple, 
                            set
                        )
                    ):
                        val = [pd.to_numeric(v, errors="coerce") for v in val]
                    else:
                        val = pd.to_numeric(val, errors="coerce")
            except Exception:
                msg = (
                    "⚠️ [FETCH] Failed to apply WHERE config due to condition "
                    f"{idx} cannot cast value to raw value used.")
                print(msg)
                logging.warning(msg)

            if op == "=":
                df = df[series == val]
            elif op == "!=":
                df = df[series != val]
            elif op == ">":
                df = df[series > val]
            elif op == ">=":
                df = df[series >= val]
            elif op == "<":
                df = df[series < val]
            elif op == "<=":
                df = df[series <= val]
            elif op == "in":
                df = df[series.isin(val)]
            elif op == "not in":
                df = df[~series.isin(val)]
            elif op == "is null":
                df = df[series.isna()]
            elif op == "is not null":
                df = df[series.notna()]
            else:
                raise ValueError(
                    "❌ [FETCH] Failed to apply WHERE config due to unsupported operator "
                    f"{op} in condition "
                    f"{idx}.")
        return df

    # 1.2.4. Apply SELECT query config
    def _apply_select_config(
            self, 
            df: pd.DataFrame, 
            config: dict
        ) -> pd.DataFrame:

        select = config.get("select")
        if not select:
            return df  
        
        if isinstance(select, list):
            return df[[c for c in select if c in df.columns]]
        
        if isinstance(select, dict):
            fields = select.get("fields")
            exclude = select.get("exclude")
            if fields:
                df = df[[c for c in fields if c in df.columns]]
            if exclude:
                df = df.drop(columns=[c for c in exclude if c in df.columns])
            return df
        return df

    # 1.2.5. Apply AGGREGATE query config
    def _apply_aggregate_config(
            self, 
            df: pd.DataFrame, 
            config: dict
        ) -> pd.DataFrame:
        
        agg = config.get("aggregate")
        if not agg:
            return df

        group_by = agg.get("group_by", [])
        metrics = agg.get("metrics", {})

        return (
            df.groupby(group_by, dropna=False)
              .agg(metrics)
              .reset_index()
        )

    # 1.2.6. Apply ORDER BY query config
    def _apply_order_by(
            self, 
            df: pd.DataFrame, 
            config: dict
        ) -> pd.DataFrame:
        
        order_by = config.get("order_by")
        if not order_by:
            return df

        valid_order_by = [
            o for o in order_by
            if "field" in o and o["field"] in df.columns
        ]

        if not valid_order_by:
            raise ValueError(
                "❌ [FETCH] order_by is defined but no valid field found in dataframe."
            )

        cols = [o["field"] for o in valid_order_by]
        asc = [o.get("direction", "asc") == "asc" for o in valid_order_by]

        return df.sort_values(by=cols, ascending=asc)

    # 1.2.7. Apply LIMIT query config
    def _apply_limit_config(
            self, 
            df: pd.DataFrame, 
            config: dict
        ) -> pd.DataFrame:
        
        limit = config.get("limit")
        if limit is None:
            return df
        
        return df.head(limit)

    # 1.2.8. Apply SAMPLE query config
    def _apply_sample_config(
            self, 
            df: pd.DataFrame, 
            config: dict
        ) -> pd.DataFrame:
        
        sample = config.get("sample")
        if not sample:
            return df

        method = sample.get("method")
        value = sample.get("value")

        if method == "fraction":
            return df.sample(frac=value)
        if method == "n":
            return df.sample(n=value)
        raise ValueError(
            "❌ [FETCH] Failed to apply SAMPLE query config due to unvalid sample method " 
            f"{method} in fetch config instead of fraction or n."
        )

# 1.3. Entrypoint
    def fetch(
            self, 
            config: dict | None = None
        ) -> pd.DataFrame:
        
        config = self._validate_fetch_config(config or {})

        df = self._fetch()

        df = self._apply_where_config(df, config)
        df = self._apply_select_config(df, config)
        df = self._apply_aggregate_config(df, config)
        df = self._apply_order_by(df, config)
        df = self._apply_limit_config(df, config)
        df = self._apply_sample_config(df, config)

        return df