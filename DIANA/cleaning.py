from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd

try:
    import janitor  # noqa: F401
except Exception:  # noqa: BLE001
    janitor = None  # type: ignore[assignment]

try:
    from feature_engine.imputation import CategoricalImputer, MeanMedianImputer
except Exception:  # noqa: BLE001
    CategoricalImputer = None  # type: ignore[assignment]
    MeanMedianImputer = None  # type: ignore[assignment]

from sklearn.impute import SimpleImputer

try:
    from sklearn.experimental import enable_iterative_imputer  # noqa: F401
    from sklearn.impute import IterativeImputer
except Exception:  # noqa: BLE001
    IterativeImputer = None  # type: ignore[assignment]

try:
    from cleanlab.filter import find_label_issues
except Exception:  # noqa: BLE001
    find_label_issues = None  # type: ignore[assignment]

try:
    import dedupe  # type: ignore[import-not-found]
except Exception:  # noqa: BLE001
    dedupe = None  # type: ignore[assignment]


OutlierStrategy = Literal["clip", "keep"]


@dataclass(frozen=True)
class CleanConfig:
    drop_col_missingness_ge: float = 0.85
    drop_row_missingness_ge: float = 0.85
    try_iterative_imputer: bool = True
    outlier_strategy: OutlierStrategy = "clip"
    clip_iqr_k: float = 3.0
    max_unique_for_case_standardize: int = 200
    enable_cleanlab: bool = True
    dedupe_settings_path: str | None = None


def _as_na_strings(s: pd.Series) -> pd.Series:
    if not (pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s)):
        return s
    x = s.astype("string")
    x = x.str.strip()
    x = x.str.replace(r"\s+", " ", regex=True)
    x = x.replace(
        {
            "": pd.NA,
            "na": pd.NA,
            "n/a": pd.NA,
            "nan": pd.NA,
            "null": pd.NA,
            "none": pd.NA,
            "?": pd.NA,
            "-": pd.NA,
            "--": pd.NA,
        }
    )
    return x


def _maybe_parse_datetime(s: pd.Series, *, min_parse_rate: float = 0.8) -> pd.Series:
    if not (pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s)):
        return s
    x = s.astype("string")
    if x.dropna().empty:
        return s
    parsed = pd.to_datetime(x, errors="coerce")
    parse_rate = float(parsed.notna().mean())
    if parse_rate >= min_parse_rate:
        return parsed
    return s


def _maybe_to_numeric_from_strings(s: pd.Series, *, min_parse_rate: float = 0.9) -> pd.Series:
    if not (pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s)):
        return s
    x = s.astype("string")
    if x.dropna().empty:
        return s
    cleaned = (
        x.str.replace(r"[,\s]", "", regex=True)
        .str.replace(r"[%$₹€£]", "", regex=True)
        .str.replace(r"^\((.*)\)$", r"-\1", regex=True)
    )
    numeric = pd.to_numeric(cleaned, errors="coerce")
    parse_rate = float(numeric.notna().mean())
    if parse_rate >= min_parse_rate:
        return numeric
    return s


def _normalize_abbreviations(s: pd.Series) -> pd.Series:
    if not (pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s)):
        return s
    x = s.astype("string")
    x = x.str.replace(r"^([A-Za-z]\.){2,}$", lambda m: m.group(0).replace(".", ""), regex=True)
    return x


def _standardize_case_for_low_cardinality(s: pd.Series, *, max_unique: int) -> pd.Series:
    if not (pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s)):
        return s
    x = s.astype("string")
    non_null = x.dropna()
    if non_null.empty:
        return x
    uniq = non_null.nunique(dropna=True)
    if uniq > max_unique:
        return x

    # If values are mostly one casing already, enforce it.
    upper_rate = float((non_null == non_null.str.upper()).mean())
    lower_rate = float((non_null == non_null.str.lower()).mean())
    if upper_rate >= 0.8:
        return x.str.upper()
    if lower_rate >= 0.8:
        return x.str.lower()
    return x


def _numeric_impute_strategy(s: pd.Series) -> Literal["mean", "median"]:
    x = pd.to_numeric(s, errors="coerce")
    non_null = x.dropna()
    if non_null.empty:
        return "median"

    skew = float(non_null.skew()) if non_null.size >= 3 else 0.0
    q1 = float(non_null.quantile(0.25))
    q3 = float(non_null.quantile(0.75))
    iqr = q3 - q1
    if iqr == 0:
        outlier_frac = 0.0
    else:
        lo = q1 - 1.5 * iqr
        hi = q3 + 1.5 * iqr
        outlier_frac = float(((non_null < lo) | (non_null > hi)).mean())

    if abs(skew) > 1.0 or outlier_frac > 0.05:
        return "median"
    return "mean"


def _clip_outliers_iqr(df: pd.DataFrame, numeric_cols: list[str], *, k: float) -> dict[str, Any]:
    clipped_cols: list[str] = []
    for c in numeric_cols:
        s = pd.to_numeric(df[c], errors="coerce")
        non_null = s.dropna()
        if non_null.empty:
            continue
        q1 = float(non_null.quantile(0.25))
        q3 = float(non_null.quantile(0.75))
        iqr = q3 - q1
        if iqr == 0:
            continue
        lo = q1 - k * iqr
        hi = q3 + k * iqr
        before = s.copy()
        df[c] = s.clip(lower=lo, upper=hi)
        if not before.equals(df[c]):
            clipped_cols.append(c)
    return {"clipped_columns": clipped_cols, "clip_iqr_k": k}


def clean_dataframe(df: pd.DataFrame, *, config: CleanConfig | None = None) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Deterministic 'raw -> clean' transform:
    - clean column names
    - remove duplicates
    - normalize common missing tokens
    - parse datetimes and numeric strings
    - drop very-empty rows/columns
    - type-aware imputation (categorical mode, numeric mean/median, datetime ffill/bfill)
    - optional numeric IterativeImputer (regression-based filling) when feasible
    - optional outlier clipping for numeric columns
    - QA summary report returned as a dict
    """
    cfg = config or CleanConfig()
    report: dict[str, Any] = {"steps": [], "notes": []}

    raw_shape = (int(df.shape[0]), int(df.shape[1]))
    report["raw_shape"] = raw_shape

    df2 = df.copy()

    # Column name hygiene (pyjanitor if available).
    if janitor is not None:
        try:
            df2 = df2.clean_names()
            report["steps"].append({"clean_names": True})
        except Exception as exc:  # noqa: BLE001
            report["steps"].append({"clean_names": False, "error": str(exc)})

    # Remove exact duplicate rows.
    dup_count = int(df2.duplicated().sum())
    if dup_count:
        df2 = df2.drop_duplicates().reset_index(drop=True)
    report["steps"].append({"drop_duplicates": {"removed": dup_count}})

    # Normalize missing tokens and basic structural cleanup for string-like cols.
    for col in list(df2.columns):
        s = df2[col]
        s = _as_na_strings(s)
        s = _normalize_abbreviations(s)
        s = _standardize_case_for_low_cardinality(s, max_unique=cfg.max_unique_for_case_standardize)
        df2[col] = s

    report["steps"].append({"normalize_strings": True})

    # Type inference attempts.
    for col in list(df2.columns):
        df2[col] = _maybe_parse_datetime(df2[col])
        df2[col] = _maybe_to_numeric_from_strings(df2[col])

    report["steps"].append({"infer_types": True})

    # Drop very-empty columns/rows.
    col_missing = df2.isna().mean()
    drop_cols = [c for c in df2.columns if float(col_missing[c]) >= cfg.drop_col_missingness_ge]
    if drop_cols:
        df2 = df2.drop(columns=drop_cols)
    report["steps"].append(
        {
            "drop_sparse_columns": {
                "threshold": cfg.drop_col_missingness_ge,
                "dropped": [str(c) for c in drop_cols],
            }
        }
    )

    if not df2.empty:
        row_missing = df2.isna().mean(axis=1)
        drop_rows = int((row_missing >= cfg.drop_row_missingness_ge).sum())
        if drop_rows:
            df2 = df2.loc[row_missing < cfg.drop_row_missingness_ge].reset_index(drop=True)
        report["steps"].append(
            {
                "drop_sparse_rows": {
                    "threshold": cfg.drop_row_missingness_ge,
                    "dropped": drop_rows,
                }
            }
        )

    # Imputation.
    datetime_cols = [c for c in df2.columns if pd.api.types.is_datetime64_any_dtype(df2[c])]
    numeric_cols = [c for c in df2.columns if pd.api.types.is_numeric_dtype(df2[c])]
    categorical_cols = [c for c in df2.columns if c not in set(datetime_cols) | set(numeric_cols)]

    report["dtypes"] = {
        "datetime": [str(c) for c in datetime_cols],
        "numeric": [str(c) for c in numeric_cols],
        "categorical": [str(c) for c in categorical_cols],
    }

    # Datetimes: forward/back fill within column.
    for c in datetime_cols:
        df2[c] = df2[c].ffill().bfill()
    if datetime_cols:
        report["steps"].append({"impute_datetime": {"strategy": "ffill_bfill", "cols": datetime_cols}})

    # Categorical: mode via SimpleImputer / Feature-engine.
    if categorical_cols:
        used_feature_engine = False
        if CategoricalImputer is not None:
            try:
                # Ensure all configured categorical columns are actually 'object' or 'category'
                safe_cats: list[str] = []
                for c in categorical_cols:
                    s = df2[c]
                    if not (
                        pd.api.types.is_object_dtype(s)
                        or pd.api.types.is_categorical_dtype(s)
                    ):
                        df2[c] = s.astype("object")
                    safe_cats.append(c)

                fe_cat = CategoricalImputer(
                    imputation_method="frequent",
                    variables=safe_cats,
                )
                df2 = fe_cat.fit_transform(df2)
                report["steps"].append(
                    {
                        "impute_categorical": {
                            "library": "feature_engine",
                            "strategy": "frequent",
                            "cols": safe_cats,
                        }
                    }
                )
                used_feature_engine = True
            except Exception as exc:  # noqa: BLE001
                report["notes"].append(
                    f"Feature-engine categorical imputer failed; "
                    f"falling back to sklearn SimpleImputer: {exc}"
                )

        if not used_feature_engine:
            # sklearn's SimpleImputer does not understand pandas' pd.NA; convert to np.nan
            # and ensure object dtype to avoid ambiguous NA behaviour.
            cat_block = df2[categorical_cols].copy()
            cat_block = cat_block.astype("object")
            cat_block = cat_block.where(~cat_block.isna(), np.nan)

            imp_cat = SimpleImputer(strategy="most_frequent")
            df2[categorical_cols] = imp_cat.fit_transform(cat_block)
            report["steps"].append(
                {"impute_categorical": {"library": "sklearn", "strategy": "most_frequent", "cols": categorical_cols}}
            )

    # Numeric: try IterativeImputer for multi-col numeric, otherwise per-column mean/median.
    used_iterative = False
    if (
        cfg.try_iterative_imputer
        and IterativeImputer is not None
        and len(numeric_cols) >= 2
        and float(df2[numeric_cols].isna().mean().mean()) > 0.0
    ):
        try:
            it = IterativeImputer(
                random_state=0,
                sample_posterior=False,
                max_iter=10,
                initial_strategy="median",
            )
            df2[numeric_cols] = it.fit_transform(df2[numeric_cols])
            used_iterative = True
            report["steps"].append({"impute_numeric": {"strategy": "iterative_imputer", "cols": numeric_cols}})
        except Exception as exc:  # noqa: BLE001
            report["notes"].append(f"IterativeImputer failed; falling back to per-column rules: {exc}")

    if numeric_cols and not used_iterative:
        per_col: dict[str, str] = {}
        for c in numeric_cols:
            per_col[str(c)] = _numeric_impute_strategy(df2[c])

        if MeanMedianImputer is not None:
            mean_cols = [c for c in numeric_cols if per_col[str(c)] == "mean"]
            median_cols = [c for c in numeric_cols if per_col[str(c)] == "median"]
            if mean_cols:
                df2 = MeanMedianImputer(imputation_method="mean", variables=mean_cols).fit_transform(df2)
            if median_cols:
                df2 = MeanMedianImputer(imputation_method="median", variables=median_cols).fit_transform(df2)
            report["steps"].append(
                {
                    "impute_numeric": {
                        "library": "feature_engine",
                        "strategy": "per_column_mean_or_median",
                        "per_column": per_col,
                    }
                }
            )
        else:
            for c in numeric_cols:
                strat = per_col[str(c)]
                imp = SimpleImputer(strategy=strat)
                df2[[c]] = imp.fit_transform(df2[[c]])
            report["steps"].append(
                {
                    "impute_numeric": {
                        "library": "sklearn",
                        "strategy": "per_column_mean_or_median",
                        "per_column": per_col,
                    }
                }
            )

    # Outlier detection (PyOD) - report-only (does not remove rows).
    # Use a row sample to keep runtime predictable on large datasets.
    if numeric_cols and len(numeric_cols) >= 2 and len(df2) >= 50:
        try:
            from pyod.models.iforest import IForest
            from sklearn.preprocessing import StandardScaler
            sample_df = df2[numeric_cols]
            max_rows = 2000
            if len(sample_df) > max_rows:
                sample_df = sample_df.sample(n=max_rows, random_state=0)

            X = sample_df.to_numpy(dtype=float)
            X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
            Xs = StandardScaler().fit_transform(X)

            clf = IForest(contamination=0.05, random_state=0)
            clf.fit(Xs)
            labels = clf.labels_.astype(int)  # 1 = outlier, 0 = inlier
            sample_indices = sample_df.index.to_list()
            outlier_idx = [int(sample_indices[i]) for i, v in enumerate(labels.tolist()) if v == 1]
            report["steps"].append(
                {
                    "outlier_detection": {
                        "library": "pyod",
                        "model": "IForest",
                        "contamination": 0.05,
                        "n_outliers": int(np.sum(labels == 1)),
                        "outlier_row_indices": outlier_idx[:200],
                    }
                }
            )
        except Exception as exc:  # noqa: BLE001
            report["notes"].append(f"PyOD outlier detection skipped: {exc}")

    # Outlier handling (default: clip via IQR).
    if numeric_cols and cfg.outlier_strategy == "clip":
        report["steps"].append({"outliers": _clip_outliers_iqr(df2, numeric_cols, k=cfg.clip_iqr_k)})
    else:
        report["steps"].append({"outliers": {"strategy": "keep"}})

    # Raw vs clean diff summary (per column).
    diff_summary: dict[str, Any] = {}
    common_cols = [c for c in df2.columns if c in df.columns]
    for c in common_cols:
        raw_s = df[c]
        clean_s = df2[c]
        entry: dict[str, Any] = {
            "raw_non_null": int(raw_s.notna().sum()),
            "clean_non_null": int(clean_s.notna().sum()),
            "raw_missing": int(raw_s.isna().sum()),
            "clean_missing": int(clean_s.isna().sum()),
        }

        if pd.api.types.is_numeric_dtype(clean_s):
            raw_num = pd.to_numeric(raw_s, errors="coerce")
            clean_num = pd.to_numeric(clean_s, errors="coerce")
            entry.update(
                {
                    "kind": "numeric",
                    "raw_min": float(raw_num.min()) if raw_num.notna().any() else None,
                    "raw_max": float(raw_num.max()) if raw_num.notna().any() else None,
                    "clean_min": float(clean_num.min()) if clean_num.notna().any() else None,
                    "clean_max": float(clean_num.max()) if clean_num.notna().any() else None,
                }
            )
        elif pd.api.types.is_datetime64_any_dtype(clean_s):
            raw_dt = pd.to_datetime(raw_s, errors="coerce")
            clean_dt = pd.to_datetime(clean_s, errors="coerce")
            entry.update(
                {
                    "kind": "datetime",
                    "raw_min": raw_dt.min(),
                    "raw_max": raw_dt.max(),
                    "clean_min": clean_dt.min(),
                    "clean_max": clean_dt.max(),
                }
            )
        else:
            raw_vals = raw_s.astype("string").dropna()
            clean_vals = clean_s.astype("string").dropna()
            entry.update(
                {
                    "kind": "categorical_or_text",
                    "raw_unique": int(raw_vals.nunique()),
                    "clean_unique": int(clean_vals.nunique()),
                    "raw_top_values": raw_vals.value_counts().head(3).to_dict(),
                    "clean_top_values": clean_vals.value_counts().head(3).to_dict(),
                }
            )

        diff_summary[str(c)] = entry

    report["diff_summary"] = diff_summary

    # Label-noise exploration via Cleanlab (report-only).
    if cfg.enable_cleanlab and find_label_issues is not None and len(df2) >= 50:
        try:
            # Heuristically pick a label column.
            label_candidates = ["label", "target", "y", "class"]
            label_col: Any | None = None
            for name in label_candidates:
                for col in df2.columns:
                    if str(col).strip().lower() == name:
                        label_col = col
                        break
                if label_col is not None:
                    break

            if label_col is not None:
                y = df2[label_col]
                if pd.Series(y).nunique(dropna=True) >= 2:
                    feature_cols = [
                        c for c in df2.columns if c != label_col and pd.api.types.is_numeric_dtype(df2[c])
                    ]
                    if feature_cols:
                        from sklearn.ensemble import RandomForestClassifier

                        sample_df = df2[feature_cols + [label_col]].dropna()
                        if len(sample_df) >= 50:
                            max_rows = 2000
                            if len(sample_df) > max_rows:
                                sample_df = sample_df.sample(n=max_rows, random_state=0)

                            X = sample_df[feature_cols].to_numpy(dtype=float)
                            y_arr = sample_df[label_col].to_numpy()

                            clf = RandomForestClassifier(
                                n_estimators=100,
                                random_state=0,
                                n_jobs=-1,
                            )
                            clf.fit(X, y_arr)
                            pred_probs = clf.predict_proba(X)

                            idx_issues = find_label_issues(
                                labels=y_arr,
                                pred_probs=pred_probs,
                                return_indices_ranked_by="self_confidence",
                            )
                            sample_indices = sample_df.index.to_list()
                            bad_rows = [int(sample_indices[i]) for i in idx_issues[:200]]

                            report["steps"].append(
                                {
                                    "cleanlab_label_issues": {
                                        "label_column": str(label_col),
                                        "n_issues": len(idx_issues),
                                        "issue_row_indices": bad_rows,
                                    }
                                }
                            )
        except Exception as exc:  # noqa: BLE001
            report["notes"].append(f"Cleanlab label issue detection skipped: {exc}")

    # Fuzzy-duplicate suggestions via Dedupe (report-only; requires a pre-trained settings file).
    if dedupe is not None and cfg.dedupe_settings_path:
        try:
            settings_path = Path(cfg.dedupe_settings_path)
            if settings_path.exists():
                deduper = dedupe.StaticDedupe(str(settings_path))
                fields = [f["field"] for f in deduper.data_model["fields"]]

                records: dict[str, dict[str, str]] = {}
                for idx, row in df2.iterrows():
                    rec: dict[str, str] = {}
                    for fld in fields:
                        val = row.get(fld, "")
                        if pd.isna(val):
                            rec[fld] = ""
                        else:
                            rec[fld] = str(val)
                    records[str(idx)] = rec

                clustered_dupes = deduper.partition(records, threshold=0.5)
                clusters: list[dict[str, Any]] = []
                for cluster, score in clustered_dupes:
                    if len(cluster) <= 1:
                        continue
                    clusters.append(
                        {
                            "score": float(score),
                            "row_indices": [int(rid) for rid in cluster],
                        }
                    )

                report["steps"].append(
                    {
                        "dedupe_clusters": {
                            "settings_path": str(settings_path),
                            "n_suggested_clusters": len(clusters),
                            "clusters": clusters[:50],
                        }
                    }
                )
        except Exception as exc:  # noqa: BLE001
            report["notes"].append(f"Dedupe fuzzy duplicate detection skipped: {exc}")

    # Final QA.
    report["clean_shape"] = (int(df2.shape[0]), int(df2.shape[1]))
    report["qa"] = {
        "missing_values_total": int(df2.isna().sum().sum()),
        "duplicate_rows_total": int(df2.duplicated().sum()) if not df2.empty else 0,
    }

    return df2, report

