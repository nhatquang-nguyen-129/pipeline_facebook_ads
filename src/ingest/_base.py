import os
import sys
from abc import (
    ABC,
    abstractmethod
)
import logging
import pandas as pd
sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), "../../"
        )
    )
)

class BaseIngest(ABC):
    """
    Abstract base class for ingesting data into various backends
    ---------
    Workflow:
        1. Trigger ingest               : Routes the request to the correct backend ingester
        2. Validate ingest config       : Ensures config structure & allowed keys
        3. Apply runtime overrides      : Merge runtime options into config
        4. Validate dataframe contract  : Validate dataframe against constraints
        5. Check table existence        : Backend-specific existence check
        6. Create new table (optional)  : Backend-specific table creation
        7. Apply conflict policy        : INSERT / UPSERT (including delete phase if needed)
        8. Write table data             : Load data into target backend
    ----------
    Parameters
        backend : str
            The type of backend to ingest data into (e.g., 'bigquery', 'local')
    """

    ALLOWED_CONFIG_KEYS = {
        "write",
        "conflict",
        "constraints",
        "options",
    }

# 1.1. Initialize
    def __init__(
        self,
        backend: str,
    ) -> None:
        self.backend = backend.lower()

# 1.2. Workflow

    # 1.2.1. Validate ingest config
    def _validate_ingest_config(
        self,
        config: dict,
    ) -> dict:

        if not isinstance(config, dict):
            raise TypeError(
                "❌ [INGEST] Failed to validate ingest config due to input must be a dict."
            )

        unknown_keys = set(config) - self.ALLOWED_CONFIG_KEYS
        if unknown_keys:
            raise ValueError(
                "❌ [INGEST] Failed to validate ingest config due to unsupported key(s) "
                f"{unknown_keys}. Allowed keys are {self.ALLOWED_CONFIG_KEYS}."
            )

        for key in self.ALLOWED_CONFIG_KEYS:
            config.setdefault(key, {})

        return config

    # 1.2.2. Apply runtime overrides
    def _apply_runtime_overrides(
        self,
        config: dict,
        overrides: dict,
    ) -> dict:

        if overrides:
            config["options"].update(overrides)

        return config

    # 1.2.3. Validate dataframe contract
    def _validate_df_contract(
        self,
        df: pd.DataFrame,
        config: dict,
    ) -> None:

        if df is None:
            raise ValueError(
                "❌ [INGEST] Failed to validate dataframe contract due to None dataframe."
            )

        if df.empty and config["constraints"].get("expect_non_empty", True):
            raise ValueError(
                "❌ [INGEST] Failed to validate dataframe contract due to empty dataframe is not allowed."
            )

        required_keys = config["constraints"].get("require_keys") or []
        missing = set(required_keys) - set(df.columns)
        if missing:
            raise ValueError(
                "❌ [INGEST] Failed to validate dataframe contract due to missing required column(s) "
                f"{missing}."
            )

    # 1.2.4. Check table existence
    @abstractmethod
    def _check_table_exists(
        self,
        direction: str,
    ) -> bool:
        raise NotImplementedError

    # 1.2.5. Create new table if not exist
    @abstractmethod
    def _create_new_table(
        self,
        direction: str,
        df: pd.DataFrame,
        config: dict,
    ) -> None:
        raise NotImplementedError

    # 1.2.6. Apply policy conflict
    @abstractmethod
    def _apply_conflict_policy(
        self,
        direction: str,
        df: pd.DataFrame,
        config: dict,
    ) -> None:
        raise NotImplementedError

    # 1.2.7. Write data to table
    @abstractmethod
    def _write_table_data(
        self,
        direction: str,
        df: pd.DataFrame,
        config: dict,
    ) -> None:
        raise NotImplementedError

# 1.3. Entrypoint
    def ingest(
        self,
        df: pd.DataFrame,
        direction: str,
        config: dict | None = None,
        **kwargs,
    ) -> None:

        if not direction or not isinstance(direction, str):
            raise ValueError(
                "❌ [INGEST] Failed to ingest due to direction must be a valid string."
            )

        # Trigger to validate config
        config = self._validate_ingest_config(config or {})

        # Trigger to apply runtime overrides
        config = self._apply_runtime_overrides(config, kwargs)

        # Trigger to validate dataframe
        self._validate_df_contract(df, config)

        # Trigger to check table existence
        table_exists = self._check_table_exists(direction)

        # Trigger to create table if not exist
        if not table_exists:
            if not config["options"].get("allow_table_create", True):
                raise RuntimeError(
                    "❌ [INGEST] Failed to ingest due to direction "
                    f"{direction} does not exist and auto-create is disabled."
                )

            self._create_new_table(
                direction,
                df,
                config,
            )

        # Trigger to apply conflict policy
        self._apply_conflict_policy(
            direction,
            df,
            config,
        )

        # Trigger to write data to table
        self._write_table_data(
            direction,
            df,
            config,
        )