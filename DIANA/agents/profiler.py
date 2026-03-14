from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class DatasetProfile:
    columns: list[str]
    value_hints: dict[str, list[str]]


class ProfilerAgent:
    """
    "ILM Profiler" style agent:
    inspects the dataset and extracts small hints for downstream agents.
    """

    def profile(self, df: pd.DataFrame, *, max_values_per_col: int = 20) -> DatasetProfile:
        value_hints: dict[str, list[str]] = {}
        for col in df.columns:
            s = df[col]
            if not (pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s)):
                continue

            values = s.dropna().astype(str
                                       ).str.strip()
            if values.empty:
                continue

            uniq = list(pd.unique(values))[:max_values_per_col]
            if uniq:
                value_hints[str(col)] = uniq

        return DatasetProfile(columns=[str(c) for c in df.columns], value_hints=value_hints)

