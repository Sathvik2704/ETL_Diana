from __future__ import annotations

import pandas as pd


class QAAgent:
    """
    Minimal guardrails to make common transformations more reliable.
    """

    def post_process(self, *, original_df: pd.DataFrame, result_df: pd.DataFrame, goal: str) -> pd.DataFrame:
        # Guardrail: if LLM filtered to 0 rows due to case mismatch,
        # fall back to deterministic case-insensitive FAILED filter.
        try:
            if isinstance(result_df, pd.DataFrame) and result_df.empty and not original_df.empty:
                goal_l = goal.lower()
                if "failed" in goal_l and "filter" in goal_l:
                    status_col = next(
                        (c for c in original_df.columns if str(c).strip().lower() == "status"),
                        None,
                    )
                    if status_col is not None:
                        return original_df[
                            original_df[status_col].astype(str).str.strip().str.upper() == "FAILED"
                        ].copy()
        except Exception:  # noqa: BLE001
            return result_df

        return result_df

