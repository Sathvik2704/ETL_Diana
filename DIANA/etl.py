from pathlib import Path

import pandas as pd

from agents.coder import CoderAgent, PlannerAgent
from agents.executor import ExecutorAgent
from agents.profiler import ProfilerAgent
from agents.qa import QAAgent


def _apply_goal_rules(df: pd.DataFrame, goal: str) -> pd.DataFrame:
    """
    Deterministic fallback when LLM-generated code fails.

    Covers common intents:
    - remove/drop duplicates
    - remove/fill missing values
    - basic outlier removal
    - simple status filtering (failed / delivered / read)
    """
    g = goal.lower()
    out = df.copy()

    # Duplicates.
    if "drop duplicate" in g or "remove duplicate" in g or "deduplicate" in g:
        out = out.drop_duplicates().reset_index(drop=True)

    # Remove missing rows.
    if any(k in g for k in ("remove missing", "drop missing", "drop null", "drop na")):
        out = out.dropna().reset_index(drop=True)

    # Fill missing values.
    if any(k in g for k in ("fill missing", "impute", "fill null", "fill na")):
        for c in out.columns:
            s = out[c]
            if pd.api.types.is_numeric_dtype(s):
                if s.notna().any():
                    out[c] = s.fillna(s.median())
            else:
                mode = s.mode(dropna=True)
                if not mode.empty:
                    out[c] = s.fillna(mode.iloc[0])

    # Outlier removal (IQR-based) for numeric columns.
    if "outlier" in g:
        num_cols = [c for c in out.columns if pd.api.types.is_numeric_dtype(out[c])]
        for c in num_cols:
            s = out[c]
            non_null = s.dropna()
            if non_null.empty:
                continue
            q1 = float(non_null.quantile(0.25))
            q3 = float(non_null.quantile(0.75))
            iqr = q3 - q1
            if iqr == 0:
                continue
            lo = q1 - 3.0 * iqr
            hi = q3 + 3.0 * iqr
            out = out[(s >= lo) & (s <= hi)]
        out = out.reset_index(drop=True)

    # Simple status filtering for common words.
    if "filter" in g or "only" in g:
        status_col = None
        for c in out.columns:
            if str(c).strip().lower() in {"status", "delivery_status", "message_status", "state"}:
                status_col = c
                break
        if status_col is not None:
            s = out[status_col].astype(str).str.strip().str.lower()
            mask = pd.Series(False, index=out.index)
            if "failed" in g:
                mask |= s == "failed"
            if "delivered" in g:
                mask |= s == "delivered"
            if "read" in g:
                mask |= s == "read"
            if mask.any():
                out = out[mask].reset_index(drop=True)

    # Date filters like "till 30-07-2002" / "until 2002-07-30".
    if "till" in g or "until" in g or "up to" in g:
        import re

        # Look for a DD-MM-YYYY or DD/MM/YYYY or YYYY-MM-DD style date in the goal.
        m = re.search(r"(\d{1,2}[-/]\d{1,2}[-/]\d{4}|\d{4}[-/]\d{1,2}[-/]\d{1,2})", g)
        if m:
            date_str = m.group(1)
            try:
                # Be generous: try both day-first and year-first.
                cutoff = pd.to_datetime(date_str, dayfirst=True, errors="coerce")
                if pd.isna(cutoff):
                    cutoff = pd.to_datetime(date_str, dayfirst=False, errors="coerce")
            except Exception:
                cutoff = pd.NaT

            if not pd.isna(cutoff):
                # Heuristically pick a date-like column.
                date_col = None
                for c in out.columns:
                    cname = str(c).strip().lower()
                    if any(k in cname for k in ("date", "time", "timestamp", "created_at", "sent_at")):
                        date_col = c
                        break
                if date_col is None:
                    # Fallback: first datetime-like column.
                    for c in out.columns:
                        if pd.api.types.is_datetime64_any_dtype(out[c]):
                            date_col = c
                            break

                if date_col is not None:
                    s = out[date_col]
                    s_dt = pd.to_datetime(s, errors="coerce")
                    mask = s_dt <= cutoff
                    if mask.any():
                        out = out[mask].reset_index(drop=True)

    return out


def run_etl(path: str, goal: str) -> str:
    """
    Minimal ETL pipeline:
    - Load CSV into df
    - Ask LLM for transformation code
    - Execute code with df in a sandboxed local environment
    - Save result as output.csv
    """
    csv_path = Path(path)
    if not csv_path.exists():
        raise FileNotFoundError(f"Input file not found: {csv_path}")

    original_df = pd.read_csv(csv_path)

    profiler = ProfilerAgent()
    planner = PlannerAgent()
    coder = CoderAgent()
    executor = ExecutorAgent()
    qa = QAAgent()

    profile = profiler.profile(original_df)
    plan = planner.plan(goal)
    
    max_retries = 3
    df_intermediate = None
    error_msg = None
    last_code = ""

    for attempt in range(max_retries):
        try:
            code = coder.generate_code(plan, profile, error_msg=error_msg)
            last_code = code
        except Exception:  # noqa: BLE001
            break  # If LLM generation totally fails, fall back to rules
            
        try:
            # Execute LLM code on a fresh copy of original df
            execution = executor.execute(original_df.copy(), code)
            df_tmp = execution.df
            if df_tmp is None or not isinstance(df_tmp, pd.DataFrame):
                error_msg = "Executor did not return a pandas DataFrame. You MUST ensure the result is stored in `df`."
                continue
            
            df_intermediate = df_tmp
            break  # success!
        except Exception:  # noqa: BLE001
            import traceback
            error_msg = traceback.format_exc()
            
    # If the LLM loop completely failed to yield a valid dataframe, fall back to rules
    if df_intermediate is None:
        df_intermediate = _apply_goal_rules(original_df, goal)

    # Always run rule-based goal interpretation on top of the intermediate result
    # to guarantee that common operations (filtering dates, missing values, duplicates,
    # outliers, simple status filters) are applied even if the LLM code didn't.
    df_after_rules = _apply_goal_rules(df_intermediate, goal)

    df_result = qa.post_process(original_df=original_df, result_df=df_after_rules, goal=goal)

    output_path = csv_path.parent / "output.csv"
    df_result.to_csv(output_path, index=False)

    return str(output_path)

