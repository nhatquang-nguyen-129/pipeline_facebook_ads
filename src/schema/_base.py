import os
import sys
sys.path.append(
    os.path.abspath
    (os.path.join(
        os.path.dirname(__file__), "../../"
        )
    )
)
from abc import (
    ABC,
    abstractmethod
)

import pandas as pd

# 1. BASE SCHEMA CONTRACTOR
class BaseSchema(ABC):

# 1.1. Initialize
    def __init__(
            self,
            backend: str,
            config: dict
        ):
        self.backend = backend.lower()
        self.config = config or {}
        self.columns = self.config.get("columns", {})

# 1.2. Workflow

    # 1.2.1. Rename columns
    @abstractmethod
    def _rename_df_columns(
            self,
            df: pd.DataFrame
        ) -> pd.DataFrame:
        raise NotImplementedError

    # 1.2.2. Enforce schema
    @abstractmethod
    def _enforce_df_columns(
            self,
            df: pd.DataFrame
        ) -> pd.DataFrame:
        raise NotImplementedError
    
# 1.3. Entrypoint
    def schema(
            self,
            df: pd.DataFrame
        ) -> pd.DataFrame:


        df = self._rename_df_columns(df)
        df = self._enforce_df_columns(df)

        return df