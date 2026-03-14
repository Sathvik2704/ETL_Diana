from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class ExecutionResult:
    df: pd.DataFrame
    code: str


class ExecutorAgent:
    """
    Executes generated pandas code on a dataframe.
    """

    def execute(self, df: pd.DataFrame, code: str) -> ExecutionResult:
        import numpy as np
        local_env = {"df": df, "pd": pd, "np": np}
        exec(code, {}, local_env)
        df_result = local_env.get("df", df)
        return ExecutionResult(df=df_result, code=code)

