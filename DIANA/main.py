from pathlib import Path
import os
import json
import uuid
import time
import requests
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from fastapi import FastAPI, Form, HTTPException, UploadFile, File, Query, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from dotenv import load_dotenv

from etl import run_etl
from cleaning import CleanConfig, clean_dataframe

load_dotenv(override=True)  # override=True ensures .env values take precedence

from supabase import create_client, Client
SUPABASE_URL = os.environ.get("SUPABASE_URL")
# Prefer service_role key (bypasses RLS) for backend; fall back to anon key
_service_key = os.environ.get("SUPABASE_SERVICE_KEY")
_anon_key = os.environ.get("SUPABASE_KEY")
print(f"[Startup] SUPABASE_SERVICE_KEY present: {bool(_service_key)}, SUPABASE_KEY present: {bool(_anon_key)}")
if _service_key:
    print(f"[Startup] Service key starts with: {_service_key[:20]}...")
SUPABASE_KEY = _service_key or _anon_key
_key_type = "service_role" if _service_key else "anon"
supabase: Client | None = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print(f"[Startup] Supabase client initialized for {SUPABASE_URL} (using {_key_type} key)")
    except Exception as e:
        print(f"[Startup] Failed to initialize Supabase: {e}")
else:
    print(f"[Startup] WARNING: Supabase NOT configured. SUPABASE_URL={'set' if SUPABASE_URL else 'MISSING'}, Key={'set' if SUPABASE_KEY else 'MISSING'}")
    print(f"[Startup] TIP: Set SUPABASE_SERVICE_KEY in .env (from Supabase Dashboard → Settings → API → service_role key)")


BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
VERSIONS_DIR = BASE_DIR / "versions"

app = FastAPI(title="DIANA – Intelligent ETL & Analytics Platform")

# CORS: Allow React dev server + deployed frontends
_cors_origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:5174",
    "http://127.0.0.1:5174",
]
# In production, also allow the Render URL (or any custom domain)
_render_url = os.environ.get("RENDER_EXTERNAL_URL")
if _render_url:
    _cors_origins.append(_render_url)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_filename(name: str) -> str:
    cleaned = Path(name).name
    if not cleaned or cleaned in {".", ".."}:
        raise HTTPException(status_code=400, detail="Invalid filename")
    return cleaned


def _upload_path(filename: str) -> Path:
    return UPLOAD_DIR / _safe_filename(filename)


def _read_uploaded_file(path: Path) -> pd.DataFrame:
    """Read a file into a DataFrame, auto-detecting format from extension."""
    ext = path.suffix.lower()
    if ext == ".csv":
        return pd.read_csv(path)
    elif ext in (".xlsx", ".xls"):
        return pd.read_excel(path)
    elif ext == ".json":
        return pd.read_json(path)
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported file format: {ext}. Use CSV, Excel, or JSON.")


def _save_version(run_id: str, df: pd.DataFrame, label: str) -> str:
    """Save a versioned copy of the data."""
    VERSIONS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    version_name = f"{label}_{run_id}_{ts}.csv"
    version_path = VERSIONS_DIR / version_name
    df.to_csv(version_path, index=False)
    return version_name


def _build_transformation_log(report: dict) -> list[dict]:
    """Convert cleaning report steps into a user-friendly transformation log."""
    log = []
    step_num = 0
    for step_dict in report.get("steps", []):
        for action_name, detail in step_dict.items():
            step_num += 1
            description = ""
            rows_affected = 0

            if action_name == "clean_names":
                description = "Standardized column names"
            elif action_name == "drop_duplicates":
                removed = detail.get("removed", 0) if isinstance(detail, dict) else 0
                description = f"Removed {removed} duplicate rows"
                rows_affected = removed
            elif action_name == "normalize_strings":
                description = "Normalized string values (whitespace, NA tokens)"
            elif action_name == "infer_types":
                description = "Inferred column types (datetime, numeric)"
            elif action_name == "drop_sparse_columns":
                dropped = detail.get("dropped", []) if isinstance(detail, dict) else []
                description = f"Dropped {len(dropped)} sparse columns"
            elif action_name == "drop_sparse_rows":
                dropped = detail.get("dropped", 0) if isinstance(detail, dict) else 0
                description = f"Dropped {dropped} sparse rows"
                rows_affected = dropped
            elif action_name.startswith("impute"):
                strategy = detail.get("strategy", "unknown") if isinstance(detail, dict) else "unknown"
                description = f"Imputed missing values ({action_name.split('_')[-1]}, strategy: {strategy})"
            elif action_name == "outliers":
                if isinstance(detail, dict):
                    clipped = detail.get("clipped_columns", [])
                    description = f"Clipped outliers in {len(clipped)} columns"
                else:
                    description = "Outlier handling applied"
            elif action_name == "outlier_detection":
                n_outliers = detail.get("n_outliers", 0) if isinstance(detail, dict) else 0
                description = f"Detected {n_outliers} outlier rows (IForest)"
                rows_affected = n_outliers
            elif action_name == "cleanlab_label_issues":
                n_issues = detail.get("n_issues", 0) if isinstance(detail, dict) else 0
                description = f"Detected {n_issues} potential label issues"
            else:
                description = action_name.replace("_", " ").title()

            log.append({
                "step": step_num,
                "action": description,
                "rows_affected": rows_affected,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
    return log

# ---------------------------------------------------------------------------
# Supabase Integration Helpers
# ---------------------------------------------------------------------------

def get_current_user_optional(authorization: str = Header(None)):
    if not supabase or not authorization or not authorization.startswith("Bearer "):
        return None
    token = authorization.split(" ")[1]
    try:
        res = supabase.auth.get_user(token)
        return res.user
    except Exception:
        return None

def _upload_to_supabase(local_file_path: Path) -> str | None:
    if not supabase: return None
    bucket = "datasets"
    file_name = f"{uuid.uuid4().hex[:8]}_{local_file_path.name}"
    try:
        # Use simple upload
        supabase.storage.from_(bucket).upload(file_name, str(local_file_path), {"upsert": "true"})
        return supabase.storage.from_(bucket).get_public_url(file_name)
    except Exception as e:
        print(f"Supabase upload error: {e}")
        return None

def _record_history(user_id: str, run_id: str, original_filename: str, cleaned_file_url: str | None, report_file_url: str | None, transformation_log: list):
    if not supabase:
        print("[Supabase] Client not initialized - skipping history record")
        return
    try:
        data = {
            "user_id": user_id,
            "run_id": run_id,
            "original_filename": original_filename,
            "cleaned_file_url": cleaned_file_url,
            "report_file_url": report_file_url,
            "transformation_log": transformation_log
        }
        print(f"[Supabase] Inserting history: user_id={user_id}, run_id={run_id}, filename={original_filename}")
        result = supabase.table("transform_history").insert(data).execute()
        print(f"[Supabase] Insert successful: {result.data}")
    except Exception as e:
        import traceback
        print(f"[Supabase] Failed to record history: {e}")
        print(f"[Supabase] Full traceback:\n{traceback.format_exc()}")


# ---------------------------------------------------------------------------
# Original endpoints (updated)
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    html_path = BASE_DIR / "index.html"
    if html_path.exists():
        return html_path.read_text(encoding="utf-8")
    return "<h1>DIANA ETL Platform</h1><p>Use the React frontend at localhost:5173</p>"

@app.get("/history")
async def get_history(user = Depends(get_current_user_optional)) -> JSONResponse:
    if not supabase or not user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        res = supabase.table("transform_history").select("*").eq("user_id", user.id).order("timestamp", desc=True).execute()
        return JSONResponse({"history": res.data})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@app.post("/process")
async def process(
    file: UploadFile = File(...),
    goal: str = Form(...),
    user = Depends(get_current_user_optional)
) -> JSONResponse:
    """
    Accepts a file upload and a natural-language goal, then runs the ETL pipeline.
    Now supports CSV, Excel, and JSON files.
    """
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    input_path = _upload_path(file.filename)
    contents = await file.read()
    with open(input_path, "wb") as f:
        f.write(contents)

    # For non-CSV files, convert to CSV first for the ETL pipeline
    ext = input_path.suffix.lower()
    if ext != ".csv":
        df_input = _read_uploaded_file(input_path)
        csv_path = input_path.with_suffix(".csv")
        df_input.to_csv(csv_path, index=False)
        input_path = csv_path

    output_file = run_etl(str(input_path), goal)
    output_filename = Path(output_file).name
    run_id = uuid.uuid4().hex[:10]

    # Save version
    try:
        df_out = pd.read_csv(output_file)
        _save_version(run_id, df_out, "processed")
    except Exception:
        pass

    artifacts: dict[str, str] = {}
    warnings: list[str] = []
    transformation_log = [
        {"step": 1, "action": f"Uploaded file: {file.filename}", "rows_affected": 0, "timestamp": datetime.now(timezone.utc).isoformat()},
        {"step": 2, "action": f"AI Goal: {goal}", "rows_affected": 0, "timestamp": datetime.now(timezone.utc).isoformat()},
        {"step": 3, "action": "Generated transformation code via AI", "rows_affected": 0, "timestamp": datetime.now(timezone.utc).isoformat()},
        {"step": 4, "action": "Executed transformation pipeline", "rows_affected": 0, "timestamp": datetime.now(timezone.utc).isoformat()},
    ]

    # Add row count info
    try:
        df_original = _read_uploaded_file(_upload_path(file.filename)) if _upload_path(file.filename).exists() else pd.read_csv(input_path)
        df_result = pd.read_csv(output_file)
        rows_changed = abs(len(df_original) - len(df_result))
        transformation_log.append({
            "step": 5,
            "action": f"Result: {len(df_original)} → {len(df_result)} rows ({rows_changed} rows changed)",
            "rows_affected": rows_changed,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
    except Exception:
        pass

    # Visualization generation (kept from original)
    goal_l = goal.lower()
    if any(
        k in goal_l
        for k in (
            "visual", "chart", "plot", "graph", "dashboard",
            "pie", "line", "time series", "area",
            "bar", "column", "scatter", "bubble",
        )
    ):
        try:
            df_out = pd.read_csv(output_file)
            if not df_out.empty:
                import plotly.express as px

                run_tag = uuid.uuid4().hex[:8]
                stem = f"{Path(output_filename).stem}_{run_tag}"

                numeric_cols = [c for c in df_out.columns if pd.api.types.is_numeric_dtype(df_out[c])]
                categorical_cols = [
                    c for c in df_out.columns
                    if not pd.api.types.is_numeric_dtype(df_out[c])
                    and not pd.api.types.is_datetime64_any_dtype(df_out[c])
                ]

                # Professional Styling Variables
                chart_colors = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4', '#ec4899', '#84cc16']
                import plotly.io as pio
                pio.templates.default = "plotly_dark"

                # Auto-generate a pie chart if categorical columns exist
                if categorical_cols:
                    try:
                        pie_col = categorical_cols[0]
                        counts = df_out[pie_col].value_counts().reset_index()
                        counts.columns = [pie_col, "count"]
                        
                        # Fix for high-cardinality overlapping text: limit to top 7
                        if len(counts) > 7:
                            top = counts.head(7)
                            others = pd.DataFrame({pie_col: ["Others"], "count": [counts["count"][7:].sum()]})
                            counts = pd.concat([top, others], ignore_index=True)

                        fig = px.pie(
                            counts, names=pie_col, values="count", title=f"Distribution of {pie_col}",
                            color_discrete_sequence=chart_colors, hole=0.5
                        )
                        fig.update_traces(
                            textposition='inside', textinfo='percent', 
                            hoverinfo='label+percent+value', 
                            marker=dict(line=dict(color='rgba(0,0,0,0)', width=0))
                        )
                        fig.update_layout(
                            title_font_family="Inter", title_font_size=20, 
                            margin=dict(t=60, b=40, l=40, r=40),
                            paper_bgcolor="rgba(0,0,0,0)",
                            plot_bgcolor="rgba(0,0,0,0)",
                            font_color="#e2e8f0"
                        )
                        
                        pie_path = _upload_path(f"etl_pie_{stem}.html")
                        fig.write_html(pie_path, include_plotlyjs="cdn", full_html=True)
                        artifacts["pie_chart_html"] = f"/download/{pie_path.name}"
                    except Exception:
                        pass

                # Auto-generate a bar chart
                if categorical_cols and numeric_cols:
                    try:
                        cat_col = categorical_cols[0]
                        val_col = numeric_cols[0]
                        grouped = df_out.groupby(cat_col)[val_col].sum().reset_index()
                        # Sort and limit to avoid messy x-axis labels
                        grouped = grouped.sort_values(by=val_col, ascending=False)
                        if len(grouped) > 15:
                            grouped = grouped.head(15)

                        fig = px.bar(
                            grouped, x=cat_col, y=val_col, title=f"{val_col} by {cat_col}",
                            color=cat_col, color_discrete_sequence=chart_colors
                        )
                        fig.update_layout(
                            title_font_family="Inter", title_font_size=20,
                            xaxis_title=cat_col, yaxis_title=val_col,
                            paper_bgcolor='rgba(0,0,0,0)',
                            plot_bgcolor='rgba(0,0,0,0)',
                            margin=dict(t=60, b=40, l=40, r=40),
                            showlegend=False,
                            font_color="#e2e8f0"
                        )
                        fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#334155', showline=False)
                        fig.update_xaxes(showgrid=False, showline=False)
                        
                        bar_path = _upload_path(f"etl_bar_{stem}.html")
                        fig.write_html(bar_path, include_plotlyjs="cdn", full_html=True)
                        artifacts["bar_chart_html"] = f"/download/{bar_path.name}"
                    except Exception:
                        pass

            else:
                warnings.append("ETL output is empty; charts were skipped.")
        except Exception as exc:
            warnings.append(f"Visualization step failed: {exc}")

    # Upload to Supabase and save history if user is logged in
    cleaned_url = None
    if output_file and Path(output_file).exists():
        cleaned_url = _upload_to_supabase(Path(output_file))
        
    if user:
        _record_history(
            user_id=user.id,
            run_id=run_id,
            original_filename=file.filename or "unknown",
            cleaned_file_url=cleaned_url,
            report_file_url=None, 
            transformation_log=transformation_log
        )

    return JSONResponse({
        "output_file": output_file,
        "output_filename": output_filename,
        "download_url": f"/download/{output_filename}",
        "artifacts": artifacts,
        "warnings": warnings,
        "transformation_log": transformation_log,
        "run_id": run_id,
    })


@app.post("/transform")
async def transform(
    file: UploadFile = File(...),
    user = Depends(get_current_user_optional)
) -> JSONResponse:
    """
    Deterministic "raw -> clean" transform (no LLM required).
    Now supports CSV, Excel, and JSON files.
    """
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    run_id = uuid.uuid4().hex[:10]
    original_ext = Path(file.filename).suffix.lower() if file.filename else ".csv"
    input_name = f"raw_{run_id}{original_ext}"
    input_path = _upload_path(input_name)

    contents = await file.read()
    with open(input_path, "wb") as f:
        f.write(contents)

    try:
        df_raw = _read_uploaded_file(input_path)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to read file: {exc}") from exc

    df_clean, report = clean_dataframe(df_raw, config=CleanConfig())
    report["run_id"] = run_id

    cleaned_name = f"clean_{run_id}.csv"
    cleaned_path = _upload_path(cleaned_name)
    df_clean.to_csv(cleaned_path, index=False)

    # Save version
    _save_version(run_id, df_raw, "raw")
    _save_version(run_id, df_clean, "cleaned")

    report_name = f"clean_report_{run_id}.json"
    report_path = _upload_path(report_name)
    report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    # Build transformation log from cleaning report
    transformation_log = _build_transformation_log(report)

    artifacts: dict[str, str] = {}

    # Missingness plot (missingno)
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import missingno as msno

        viz_sample = df_raw.head(5000)
        fig = msno.matrix(viz_sample, figsize=(10, 4))
        fig_path = _upload_path(f"missingness_{run_id}.png")
        plt.savefig(fig_path, bbox_inches="tight", dpi=140)
        plt.close()
        artifacts["missingness_png"] = f"/download/{fig_path.name}"
    except Exception:
        pass

    # Missingness bar (plotly)
    try:
        import plotly.express as px
        miss = df_raw.isna().mean().sort_values(ascending=False)
        miss_df = pd.DataFrame({"column": miss.index.astype(str), "missing_rate": miss.values})
        fig = px.bar(miss_df, x="column", y="missing_rate", title="Missingness by column")
        html_path = _upload_path(f"missingness_{run_id}.html")
        fig.write_html(html_path, include_plotlyjs="cdn", full_html=True)
        artifacts["missingness_html"] = f"/download/{html_path.name}"
    except Exception:
        pass

    # Full profiling report (ydata-profiling)
    try:
        from ydata_profiling import ProfileReport
        prof_sample = df_clean.head(5000)
        profile = ProfileReport(prof_sample, minimal=True, explorative=True)
        profile_path = _upload_path(f"profile_{run_id}.html")
        profile.to_file(profile_path)
        artifacts["profile_html"] = f"/download/{profile_path.name}"
    except Exception:
        pass

    # Upload to Supabase and save history
    cleaned_url = None
    report_url = None
    if cleaned_path.exists():
        cleaned_url = _upload_to_supabase(cleaned_path)
    if report_path.exists():
        report_url = _upload_to_supabase(report_path)
        
    if user:
        _record_history(
            user_id=user.id,
            run_id=run_id,
            original_filename=file.filename or "unknown",
            cleaned_file_url=cleaned_url,
            report_file_url=report_url,
            transformation_log=transformation_log
        )

    return JSONResponse({
        "run_id": run_id,
        "input_file": str(input_path),
        "cleaned_file": str(cleaned_path),
        "download_cleaned_url": f"/download/{cleaned_name}",
        "download_report_url": f"/download/{report_name}",
        "artifacts": artifacts,
        "qa_summary": report.get("qa", {}),
        "transformation_log": transformation_log,
    })


# ---------------------------------------------------------------------------
# NEW: Data Quality Dashboard
# ---------------------------------------------------------------------------

@app.get("/data-quality")
async def data_quality(filename: str = Query(...)) -> JSONResponse:
    """Returns data quality metrics for a previously uploaded file."""
    path = _upload_path(filename)
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found")

    df = _read_uploaded_file(path)

    # Missing values per column
    missing = df.isnull().sum().to_dict()
    missing_pct = df.isnull().mean().mul(100).round(2).to_dict()

    # Duplicates
    duplicate_count = int(df.duplicated().sum())

    # Column types
    col_types = {str(col): str(dtype) for col, dtype in df.dtypes.items()}

    # Outliers per numeric column (IQR method)
    outliers = {}
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    for col in numeric_cols:
        s = df[col].dropna()
        if s.empty:
            continue
        q1 = float(s.quantile(0.25))
        q3 = float(s.quantile(0.75))
        iqr = q3 - q1
        if iqr == 0:
            outliers[str(col)] = 0
            continue
        lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        outliers[str(col)] = int(((s < lo) | (s > hi)).sum())

    return JSONResponse({
        "rows": len(df),
        "columns": len(df.columns),
        "missing_values": {str(k): int(v) for k, v in missing.items()},
        "missing_percent": {str(k): float(v) for k, v in missing_pct.items()},
        "duplicate_rows": duplicate_count,
        "column_types": col_types,
        "outliers_per_column": outliers,
        "total_missing": int(df.isnull().sum().sum()),
        "total_outliers": sum(outliers.values()),
    })


# ---------------------------------------------------------------------------
# NEW: Data Summary Panel
# ---------------------------------------------------------------------------

@app.get("/data-summary")
async def data_summary(filename: str = Query(...)) -> JSONResponse:
    """Returns statistical summary of a previously uploaded file."""
    path = _upload_path(filename)
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found")

    df = _read_uploaded_file(path)

    # Basic info
    col_info = []
    for col in df.columns:
        s = df[col]
        info = {
            "name": str(col),
            "dtype": str(s.dtype),
            "non_null": int(s.notna().sum()),
            "null_count": int(s.isna().sum()),
            "unique": int(s.nunique()),
        }
        if pd.api.types.is_numeric_dtype(s):
            desc = s.describe()
            info.update({
                "mean": round(float(desc.get("mean", 0)), 2),
                "std": round(float(desc.get("std", 0)), 2),
                "min": float(desc.get("min", 0)),
                "25%": float(desc.get("25%", 0)),
                "50%": float(desc.get("50%", 0)),
                "75%": float(desc.get("75%", 0)),
                "max": float(desc.get("max", 0)),
            })
        elif pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s):
            top_vals = s.value_counts().head(5).to_dict()
            info["top_values"] = {str(k): int(v) for k, v in top_vals.items()}
        col_info.append(info)

    return JSONResponse({
        "rows": len(df),
        "columns": len(df.columns),
        "column_info": col_info,
    })


# ---------------------------------------------------------------------------
# NEW: Visualization Suggestions
# ---------------------------------------------------------------------------

@app.get("/viz-suggestions")
async def viz_suggestions(filename: str = Query(...)) -> JSONResponse:
    """Analyzes the dataset and generates auto-suggested visualizations."""
    path = _upload_path(filename)
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found")

    df = _read_uploaded_file(path)
    run_tag = uuid.uuid4().hex[:8]
    charts: list[dict] = []

    import plotly.express as px

    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    categorical_cols = [
        c for c in df.columns
        if pd.api.types.is_object_dtype(df[c]) or pd.api.types.is_string_dtype(df[c])
    ]

    # 1. Histograms for numeric columns (max 4)
    for col in numeric_cols[:4]:
        try:
            fig = px.histogram(df, x=col, title=f"Distribution of {col}", nbins=30)
            chart_name = f"hist_{col}_{run_tag}.html"
            chart_path = _upload_path(chart_name)
            fig.write_html(chart_path, include_plotlyjs="cdn", full_html=True)
            charts.append({
                "type": "histogram",
                "title": f"Distribution of {col}",
                "url": f"/download/{chart_name}",
                "description": f"Shows the frequency distribution of {col}",
            })
        except Exception:
            pass

    # 2. Correlation heatmap (if 2+ numeric cols)
    if len(numeric_cols) >= 2:
        try:
            corr = df[numeric_cols].corr()
            fig = px.imshow(
                corr,
                text_auto=".2f",
                title="Correlation Heatmap",
                color_continuous_scale="RdBu_r",
                aspect="auto",
            )
            chart_name = f"corr_heatmap_{run_tag}.html"
            chart_path = _upload_path(chart_name)
            fig.write_html(chart_path, include_plotlyjs="cdn", full_html=True)
            charts.append({
                "type": "heatmap",
                "title": "Correlation Heatmap",
                "url": f"/download/{chart_name}",
                "description": "Shows correlations between numeric columns",
            })
        except Exception:
            pass

    # 3. Box plots for numeric columns (max 4)
    for col in numeric_cols[:4]:
        try:
            fig = px.box(df, y=col, title=f"Box Plot of {col}")
            chart_name = f"box_{col}_{run_tag}.html"
            chart_path = _upload_path(chart_name)
            fig.write_html(chart_path, include_plotlyjs="cdn", full_html=True)
            charts.append({
                "type": "boxplot",
                "title": f"Box Plot of {col}",
                "url": f"/download/{chart_name}",
                "description": f"Shows spread, median, and outliers of {col}",
            })
        except Exception:
            pass

    # 4. Bar chart for categorical columns (max 2)
    for col in categorical_cols[:2]:
        try:
            counts = df[col].value_counts().head(15).reset_index()
            counts.columns = [col, "count"]
            fig = px.bar(counts, x=col, y="count", title=f"Frequency of {col}")
            chart_name = f"catbar_{col}_{run_tag}.html"
            chart_path = _upload_path(chart_name)
            fig.write_html(chart_path, include_plotlyjs="cdn", full_html=True)
            charts.append({
                "type": "bar",
                "title": f"Frequency of {col}",
                "url": f"/download/{chart_name}",
                "description": f"Shows value counts for {col}",
            })
        except Exception:
            pass

    # 5. Scatter plot for first two numeric columns
    if len(numeric_cols) >= 2:
        try:
            color = categorical_cols[0] if categorical_cols else None
            fig = px.scatter(
                df, x=numeric_cols[0], y=numeric_cols[1],
                color=color,
                title=f"Scatter: {numeric_cols[0]} vs {numeric_cols[1]}",
            )
            chart_name = f"scatter_{run_tag}.html"
            chart_path = _upload_path(chart_name)
            fig.write_html(chart_path, include_plotlyjs="cdn", full_html=True)
            charts.append({
                "type": "scatter",
                "title": f"Scatter: {numeric_cols[0]} vs {numeric_cols[1]}",
                "url": f"/download/{chart_name}",
                "description": f"Relationship between {numeric_cols[0]} and {numeric_cols[1]}",
            })
        except Exception:
            pass

    return JSONResponse({"charts": charts})


# ---------------------------------------------------------------------------
# NEW: Dashboard Data (for Recharts frontend)
# ---------------------------------------------------------------------------

@app.get("/dashboard-data")
async def dashboard_data(filename: str = Query(...)) -> JSONResponse:
    """Returns structured chart data for native Recharts rendering on the frontend."""
    path = _upload_path(filename)
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found")

    df = _read_uploaded_file(path)
    charts: list[dict] = []

    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    categorical_cols = [
        c for c in df.columns
        if (pd.api.types.is_object_dtype(df[c]) or pd.api.types.is_string_dtype(df[c]))
        and df[c].nunique() <= 50
    ]
    datetime_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]

    # Also try to detect date-like text columns
    for c in df.columns:
        if c not in datetime_cols and pd.api.types.is_object_dtype(df[c]):
            sample = df[c].dropna().head(20)
            try:
                parsed = pd.to_datetime(sample, errors="coerce")
                if parsed.notna().sum() > len(sample) * 0.7:
                    df[c] = pd.to_datetime(df[c], errors="coerce")
                    datetime_cols.append(c)
                    if c in categorical_cols:
                        categorical_cols.remove(c)
            except Exception:
                pass

    VIBRANT_COLORS = [
        "#3b82f6", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6",
        "#06b6d4", "#ec4899", "#84cc16", "#f97316", "#6366f1",
        "#14b8a6", "#e11d48", "#a855f7", "#eab308", "#22c55e",
    ]

    # ── 1. Time Series / Trend (Area Chart) ──
    if datetime_cols and numeric_cols:
        dt_col = datetime_cols[0]
        val_col = numeric_cols[0]
        try:
            ts = df[[dt_col, val_col]].dropna().copy()
            ts[dt_col] = pd.to_datetime(ts[dt_col], errors="coerce")
            ts = ts.dropna()
            ts = ts.sort_values(dt_col)
            # Resample by month if enough data
            ts.set_index(dt_col, inplace=True)
            if len(ts) > 60:
                grouped = ts.resample("M").sum().reset_index()
            elif len(ts) > 14:
                grouped = ts.resample("W").sum().reset_index()
            else:
                grouped = ts.reset_index()
            grouped.columns = ["date", "value"]
            grouped["date"] = grouped["date"].dt.strftime("%b %Y")
            charts.append({
                "id": "trend",
                "type": "area",
                "title": f"Trend: {val_col}",
                "subtitle": f"{val_col} over time",
                "data": grouped.to_dict(orient="records"),
                "dataKey": "value",
                "xKey": "date",
                "color": "#3b82f6",
                "gradientColor": "#3b82f6",
            })
        except Exception:
            pass

    # ── 2. Categorical distribution (Donut charts) ──
    for i, col in enumerate(categorical_cols[:3]):
        try:
            counts = df[col].value_counts().head(10).reset_index()
            counts.columns = ["name", "value"]
            colors = VIBRANT_COLORS[:len(counts)]
            data = []
            for j, row in counts.iterrows():
                data.append({
                    "name": str(row["name"]),
                    "value": int(row["value"]),
                    "color": colors[j % len(colors)],
                })
            total = sum(d["value"] for d in data)
            # Add percentages
            for d in data:
                d["percent"] = round(d["value"] / total * 100, 1) if total else 0

            charts.append({
                "id": f"donut_{col}",
                "type": "donut",
                "title": f"{col} Distribution",
                "subtitle": f"Breakdown by {col}",
                "data": data,
                "colors": colors,
            })
        except Exception:
            pass

    # ── 3. Bar chart for top categorical values (if categorical + numeric exists) ──
    if categorical_cols and numeric_cols:
        cat_col = categorical_cols[0]
        val_col = numeric_cols[0]
        try:
            grouped = df.groupby(cat_col)[val_col].sum().sort_values(ascending=False).head(10).reset_index()
            grouped.columns = ["name", "value"]
            data = []
            for j, row in grouped.iterrows():
                data.append({
                    "name": str(row["name"]),
                    "value": float(row["value"]),
                    "color": VIBRANT_COLORS[j % len(VIBRANT_COLORS)],
                })
            charts.append({
                "id": f"bar_{cat_col}_{val_col}",
                "type": "bar",
                "title": f"{val_col} by {cat_col}",
                "subtitle": f"Top {len(data)} {cat_col} values",
                "data": data,
                "color": "#3b82f6",
            })
        except Exception:
            pass

    # ── 4. Horizontal bar for value counts (Grade distribution style) ──
    for col in categorical_cols[:2]:
        try:
            counts = df[col].value_counts().head(15).reset_index()
            counts.columns = ["name", "value"]
            data = []
            for j, row in counts.iterrows():
                data.append({
                    "name": str(row["name"]),
                    "value": int(row["value"]),
                    "color": VIBRANT_COLORS[j % len(VIBRANT_COLORS)],
                })
            charts.append({
                "id": f"hbar_{col}",
                "type": "horizontal_bar",
                "title": f"{col} Breakdown",
                "subtitle": f"Count by {col}",
                "data": data,
            })
        except Exception:
            pass

    # ── 5. Day-of-week / weekday distribution (if datetime columns exist) ──
    if datetime_cols:
        dt_col = datetime_cols[0]
        try:
            days = df[dt_col].dropna().dt.day_name().value_counts()
            day_order = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
            day_data = []
            for day in day_order:
                val = int(days.get(day, 0))
                if val > 0:
                    day_data.append({"name": day[:3], "value": val})
            if day_data:
                max_val = max(d["value"] for d in day_data)
                for d in day_data:
                    d["color"] = "#10b981" if d["value"] == max_val else "#3b82f6"
                charts.append({
                    "id": "weekday",
                    "type": "bar",
                    "title": "Distribution by Day of Week",
                    "subtitle": "Activity by weekday",
                    "data": day_data,
                    "color": "#3b82f6",
                })
        except Exception:
            pass

    # ── 6. Top-N ranked list with progress bars ──
    if categorical_cols:
        col = categorical_cols[0]
        try:
            counts = df[col].value_counts().head(5).reset_index()
            counts.columns = ["name", "value"]
            total = int(df[col].notna().sum())
            ranked_data = []
            for j, row in counts.iterrows():
                pct = round(row["value"] / total * 100, 1) if total else 0
                ranked_data.append({
                    "rank": j + 1,
                    "name": str(row["name"]),
                    "value": int(row["value"]),
                    "percent": pct,
                    "color": VIBRANT_COLORS[j % len(VIBRANT_COLORS)],
                })
            charts.append({
                "id": f"ranked_{col}",
                "type": "ranked_list",
                "title": f"Top 5 {col}",
                "subtitle": "Most frequent values",
                "data": ranked_data,
                "total": total,
            })
        except Exception:
            pass

    # ── 7. Recent daily trend (line chart) ──
    if datetime_cols and numeric_cols:
        dt_col = datetime_cols[0]
        try:
            ts = df[[dt_col]].dropna().copy()
            ts[dt_col] = pd.to_datetime(ts[dt_col], errors="coerce")
            ts = ts.dropna()
            ts["date"] = ts[dt_col].dt.date
            daily = ts.groupby("date").size().reset_index(name="count")
            daily = daily.sort_values("date").tail(14)
            daily["date"] = daily["date"].astype(str)
            charts.append({
                "id": "daily_trend",
                "type": "line",
                "title": "Recent Daily Trend",
                "subtitle": "Last 14 days activity",
                "data": daily.to_dict(orient="records"),
                "dataKey": "count",
                "xKey": "date",
                "color": "#a855f7",
            })
        except Exception:
            pass

    # ── 8. Grouped bar chart (cross-tab of two categorical cols) ──
    if len(categorical_cols) >= 2:
        cat1, cat2 = categorical_cols[0], categorical_cols[1]
        try:
            # Only if cat2 has few unique values (like gender: Male/Female)
            if df[cat2].nunique() <= 5:
                cross = pd.crosstab(df[cat1], df[cat2]).head(10).reset_index()
                data = []
                group_keys = [str(c) for c in cross.columns if str(c) != cat1]
                for _, row in cross.iterrows():
                    entry = {"name": str(row[cat1])}
                    for key in group_keys:
                        entry[key] = int(row[key])
                    data.append(entry)
                charts.append({
                    "id": f"grouped_{cat1}_{cat2}",
                    "type": "grouped_bar",
                    "title": f"{cat1} by {cat2}",
                    "subtitle": f"Grouped breakdown",
                    "data": data,
                    "keys": group_keys,
                    "colors": VIBRANT_COLORS[:len(group_keys)],
                })
        except Exception:
            pass

    # ── 9. Numeric distribution histograms ──
    for col in numeric_cols[:2]:
        try:
            values = df[col].dropna()
            if len(values) > 5:
                hist, edges = np.histogram(values, bins=min(20, len(values)))
                data = []
                for j in range(len(hist)):
                    data.append({
                        "range": f"{edges[j]:.0f}-{edges[j+1]:.0f}",
                        "value": int(hist[j]),
                    })
                charts.append({
                    "id": f"hist_{col}",
                    "type": "bar",
                    "title": f"Distribution of {col}",
                    "subtitle": f"Frequency histogram",
                    "data": data,
                    "color": "#06b6d4",
                })
        except Exception:
            pass

    return JSONResponse({"charts": charts})


# ---------------------------------------------------------------------------
# NEW: Chat With Dataset
# ---------------------------------------------------------------------------

class ChatRequest(BaseModel):
    filename: str
    question: str


@app.post("/chat")
async def chat_with_dataset(req: ChatRequest) -> JSONResponse:
    """
    Allows users to ask natural-language questions about their dataset.
    Uses Gemini to generate pandas code, executes it, and returns the answer.
    """
    path = _upload_path(req.filename)
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found")

    df = _read_uploaded_file(path)

    # Build context
    schema_info = []
    for col in df.columns:
        s = df[col]
        dtype = str(s.dtype)
        sample_vals = s.dropna().head(3).tolist()
        schema_info.append(f"  - {col} ({dtype}): sample values = {sample_vals}")
    schema_str = "\n".join(schema_info)
    summary_str = df.describe(include="all").to_string()

    prompt = f"""You are a data analyst assistant. A user has a pandas DataFrame named `df` with the following schema:

{schema_str}

Summary statistics:
{summary_str}

The DataFrame has {len(df)} rows and {len(df.columns)} columns.

User question: "{req.question}"

You have TWO output options:
1. If the question can be answered with a simple text response (e.g., "how many rows?"), respond with just the answer text.
2. If the question needs computation, write ONLY Python code that:
   - Uses the existing `df` variable
   - Stores the FINAL ANSWER as a string in a variable called `answer`
   - Does NOT print anything
   - Imports any needed libraries

If you write code, output ONLY the code. No explanations, no markdown, no backticks.
If you write a text answer, start it with "ANSWER:" followed by the answer.
"""

    try:
        api_key = os.environ.get("GEMINI_API_KEY", "")
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}"
        payload = {"contents": [{"parts": [{"text": prompt}]}]}
        raw_response = ""  # Initialize before retry loop
        last_error = None
        for i in range(3):
            try:
                time.sleep(1)  # Burst protection
                r = requests.post(url, json=payload, timeout=30)
                if r.status_code == 429:
                    time.sleep(3 * (i + 1))  # Exponential backoff
                    continue
                r.raise_for_status()
                raw_response = r.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
                break
            except Exception as e:
                last_error = e
                time.sleep(2 * (i + 1))

        if not raw_response:
            raise RuntimeError(f"AI service unavailable after retries: {last_error or 'rate limited'}")

        # Strip markdown code blocks if present
        if raw_response.startswith("```"):
            parts = raw_response.split("```")
            inner = [p for p in parts[1:] if p.strip()]
            if inner:
                body = inner[0]
                lines = body.splitlines()
                if lines and lines[0].strip().lower().startswith("python"):
                    lines = lines[1:]
                raw_response = "\n".join(lines).strip()

        # Check if it's a direct text answer
        if raw_response.upper().startswith("ANSWER:"):
            answer = raw_response[7:].strip()
        else:
            # Execute as code
            local_env = {"df": df.copy(), "pd": pd, "np": np}
            exec(raw_response, {}, local_env)
            answer = str(local_env.get("answer", "I processed your question but couldn't generate a specific answer. Please try rephrasing."))

        return JSONResponse({
            "question": req.question,
            "answer": answer,
            "code_used": raw_response if not raw_response.upper().startswith("ANSWER:") else None,
        })

    except Exception as exc:
        return JSONResponse({
            "question": req.question,
            "answer": f"Sorry, I encountered an error processing your question: {str(exc)}",
            "code_used": None,
        })


# ---------------------------------------------------------------------------
# NEW: AI Report Generator
# ---------------------------------------------------------------------------

@app.post("/generate-report")
async def generate_report(filename: str = Query(...)) -> JSONResponse:
    """Generates a comprehensive AI-powered data analysis report."""
    path = _upload_path(filename)
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found")

    df = _read_uploaded_file(path)
    run_tag = uuid.uuid4().hex[:8]

    # Gather statistics
    summary = df.describe(include="all").to_string()
    missing = df.isnull().sum().to_string()
    dtypes = df.dtypes.to_string()
    shape = f"{df.shape[0]} rows x {df.shape[1]} columns"

    # Correlation info for numeric columns
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    corr_info = ""
    if len(numeric_cols) >= 2:
        corr = df[numeric_cols].corr()
        corr_info = f"\nCorrelation matrix:\n{corr.to_string()}"

    # Duplicate info
    dup_count = int(df.duplicated().sum())

    prompt = f"""You are a professional data analyst. Generate a comprehensive, well-formatted data analysis report.

Dataset shape: {shape}
Duplicate rows: {dup_count}

Column types:
{dtypes}

Dataset summary statistics:
{summary}

Missing values per column:
{missing}
{corr_info}

Write a professional data analysis report with these sections:
1. **Dataset Overview** - describe the dataset structure and purpose
2. **Data Quality Analysis** - missing values, duplicates, data types
3. **Key Statistics** - important statistical findings  
4. **Insights & Patterns** - notable patterns, correlations, distributions
5. **Recommendations** - suggested next steps for data cleaning or analysis

Use professional language. Be specific with numbers. Format with markdown headers and bullet points.
"""

    report_text = ""  # Initialize before retry loop
    try:
        api_key = os.environ.get("GEMINI_API_KEY", "")
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}"
        payload = {"contents": [{"parts": [{"text": prompt}]}]}
        last_error = None
        for i in range(3):
            try:
                time.sleep(1)  # Burst protection
                r = requests.post(url, json=payload, timeout=30)
                if r.status_code == 429:
                    time.sleep(3 * (i + 1))  # Exponential backoff
                    continue
                r.raise_for_status()
                report_text = r.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
                break
            except Exception as e:
                last_error = e
                time.sleep(2 * (i + 1))
    except Exception as exc:
        pass  # Fall through to fallback

    if not report_text:
        # Fallback: generate a basic report without AI
        report_text = _generate_basic_report(df, shape, dup_count)

    # Save as HTML report
    html_content = f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<title>DIANA Data Analysis Report</title>
<style>
    body {{ font-family: 'Segoe UI', sans-serif; max-width: 900px; margin: 40px auto; padding: 20px;
           background: #0f172a; color: #e2e8f0; line-height: 1.7; }}
    h1 {{ color: #60a5fa; border-bottom: 2px solid #1e3a5f; padding-bottom: 10px; }}
    h2 {{ color: #93c5fd; margin-top: 30px; }}
    h3 {{ color: #bfdbfe; }}
    code {{ background: #1e293b; padding: 2px 6px; border-radius: 4px; color: #fbbf24; }}
    pre {{ background: #1e293b; padding: 16px; border-radius: 8px; overflow-x: auto; }}
    ul, ol {{ padding-left: 24px; }}
    li {{ margin-bottom: 6px; }}
    strong {{ color: #f1f5f9; }}
    .header {{ text-align: center; padding: 20px 0; }}
    .timestamp {{ color: #64748b; font-size: 0.85em; }}
</style>
</head><body>
<div class="header">
    <h1>📊 DIANA Data Analysis Report</h1>
    <p class="timestamp">Generated on {datetime.now().strftime("%B %d, %Y at %I:%M %p")}</p>
</div>
<div>{_markdown_to_html(report_text)}</div>
</body></html>"""

    html_name = f"report_{run_tag}.html"
    html_path = _upload_path(html_name)
    html_path.write_text(html_content, encoding="utf-8")

    # Generate PDF report
    pdf_url = None
    try:
        from fpdf import FPDF

        pdf = FPDF()
        pdf.add_page()
        pdf.set_auto_page_break(auto=True, margin=15)
        pdf.set_font("Helvetica", "B", 20)
        pdf.cell(0, 15, "DIANA Data Analysis Report", new_x="LMARGIN", new_y="NEXT", align="C")
        pdf.set_font("Helvetica", "", 10)
        pdf.cell(0, 8, f"Generated on {datetime.now().strftime('%B %d, %Y at %I:%M %p')}", new_x="LMARGIN", new_y="NEXT", align="C")
        pdf.ln(10)
        pdf.set_font("Helvetica", "", 11)

        # Clean markdown for PDF
        clean_text = report_text.replace("**", "").replace("##", "").replace("#", "").replace("*", "")
        for line in clean_text.split("\n"):
            line = line.strip()
            if not line:
                pdf.ln(4)
                continue
            try:
                pdf.multi_cell(0, 6, line)
            except Exception:
                pdf.multi_cell(0, 6, line.encode("ascii", "replace").decode("ascii"))

        pdf_name = f"report_{run_tag}.pdf"
        pdf_path = _upload_path(pdf_name)
        pdf.output(str(pdf_path))
        pdf_url = f"/download/{pdf_name}"
    except Exception:
        pass

    return JSONResponse({
        "report_text": report_text,
        "html_url": f"/download/{html_name}",
        "pdf_url": pdf_url,
    })


def _markdown_to_html(text: str) -> str:
    """Very basic markdown to HTML conversion."""
    import re
    lines = text.split("\n")
    html_lines = []
    in_list = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("### "):
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            html_lines.append(f"<h3>{stripped[4:]}</h3>")
        elif stripped.startswith("## "):
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            html_lines.append(f"<h2>{stripped[3:]}</h2>")
        elif stripped.startswith("# "):
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            html_lines.append(f"<h2>{stripped[2:]}</h2>")
        elif stripped.startswith("- ") or stripped.startswith("* "):
            if not in_list:
                html_lines.append("<ul>")
                in_list = True
            content = stripped[2:]
            content = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", content)
            html_lines.append(f"<li>{content}</li>")
        elif stripped.startswith("```"):
            continue
        elif stripped:
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            content = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", stripped)
            content = re.sub(r"`(.+?)`", r"<code>\1</code>", content)
            html_lines.append(f"<p>{content}</p>")
    if in_list:
        html_lines.append("</ul>")
    return "\n".join(html_lines)


def _generate_basic_report(df: pd.DataFrame, shape: str, dup_count: int) -> str:
    """Fallback report generation without AI."""
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    cat_cols = [c for c in df.columns if pd.api.types.is_object_dtype(df[c])]
    missing_total = int(df.isnull().sum().sum())
    missing_cols = {str(c): int(v) for c, v in df.isnull().sum().items() if v > 0}

    report = f"""# Data Analysis Report

## Dataset Overview
- Shape: {shape}
- Numeric columns: {len(numeric_cols)} ({', '.join(str(c) for c in numeric_cols[:5])})
- Categorical columns: {len(cat_cols)} ({', '.join(str(c) for c in cat_cols[:5])})

## Data Quality Analysis
- Total missing values: {missing_total}
- Duplicate rows: {dup_count}
- Columns with missing values: {len(missing_cols)}

## Key Statistics
"""
    for col in numeric_cols[:5]:
        s = df[col]
        report += f"- **{col}**: mean={s.mean():.2f}, median={s.median():.2f}, std={s.std():.2f}\n"

    report += "\n## Recommendations\n"
    if missing_total > 0:
        report += "- Address missing values through imputation or removal\n"
    if dup_count > 0:
        report += f"- Remove {dup_count} duplicate rows\n"
    report += "- Consider feature engineering for numeric columns\n"

    return report


# ---------------------------------------------------------------------------
# NEW: Export Options
# ---------------------------------------------------------------------------

@app.get("/export/{filename}/{fmt}")
async def export_data(filename: str, fmt: str) -> FileResponse:
    """Export data in various formats: csv, excel, json, pdf."""
    path = _upload_path(filename)
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found")

    df = _read_uploaded_file(path)
    run_tag = uuid.uuid4().hex[:8]

    if fmt == "csv":
        return FileResponse(
            path, media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{path.stem}_export.csv"'},
        )

    elif fmt == "excel":
        excel_name = f"{path.stem}_export_{run_tag}.xlsx"
        excel_path = _upload_path(excel_name)
        df.to_excel(excel_path, index=False, engine="openpyxl")
        return FileResponse(
            excel_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{excel_name}"'},
        )

    elif fmt == "json":
        json_name = f"{path.stem}_export_{run_tag}.json"
        json_path = _upload_path(json_name)
        df.to_json(json_path, orient="records", indent=2)
        return FileResponse(
            json_path, media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="{json_name}"'},
        )

    elif fmt == "pdf":
        from fpdf import FPDF

        pdf = FPDF()
        pdf.add_page("L")
        pdf.set_auto_page_break(auto=True, margin=15)
        pdf.set_font("Helvetica", "B", 14)
        pdf.cell(0, 10, f"Data Export: {path.stem}", new_x="LMARGIN", new_y="NEXT", align="C")
        pdf.ln(5)

        # Table header
        cols = list(df.columns)[:10]  # Limit to 10 columns for PDF
        col_width = max(25, (pdf.w - 20) / len(cols))
        pdf.set_font("Helvetica", "B", 8)
        for col in cols:
            pdf.cell(col_width, 8, str(col)[:15], border=1, align="C")
        pdf.ln()

        # Table rows (max 100)
        pdf.set_font("Helvetica", "", 7)
        for _, row in df.head(100).iterrows():
            for col in cols:
                val = str(row[col])[:15] if pd.notna(row[col]) else ""
                try:
                    pdf.cell(col_width, 7, val, border=1)
                except Exception:
                    pdf.cell(col_width, 7, val.encode("ascii", "replace").decode("ascii"), border=1)
            pdf.ln()

        pdf_name = f"{path.stem}_export_{run_tag}.pdf"
        pdf_path = _upload_path(pdf_name)
        pdf.output(str(pdf_path))
        return FileResponse(
            pdf_path, media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{pdf_name}"'},
        )

    else:
        raise HTTPException(status_code=400, detail=f"Unsupported format: {fmt}. Use csv, excel, json, or pdf.")


# ---------------------------------------------------------------------------
# Download handler (updated)
# ---------------------------------------------------------------------------

@app.get("/download/{filename}")
async def download_output(filename: str) -> FileResponse:
    """Download a generated artifact (CSV/HTML/PNG/JSON/XLSX/PDF)."""
    path = _upload_path(filename)
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found")

    ext = path.suffix.lower()
    media_type = {
        ".csv": "text/csv",
        ".json": "application/json",
        ".html": "text/html",
        ".htm": "text/html",
        ".png": "image/png",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".pdf": "application/pdf",
    }.get(ext, "application/octet-stream")

    disposition = "inline" if ext in {".html", ".htm", ".png"} else "attachment"
    return FileResponse(
        path,
        media_type=media_type,
        headers={"Content-Disposition": f'{disposition}; filename="{path.name}"'},
    )


# ---------------------------------------------------------------------------
# Production: Serve React frontend from frontend/dist
# ---------------------------------------------------------------------------

FRONTEND_DIST = BASE_DIR / "frontend" / "dist"

if FRONTEND_DIST.exists():
    # Serve static assets (JS, CSS, images)
    app.mount("/assets", StaticFiles(directory=str(FRONTEND_DIST / "assets")), name="static-assets")

    # Catch-all: serve index.html for any non-API route (SPA client-side routing)
    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        # If the file exists in dist, serve it (e.g., favicon.ico, manifest.json)
        file_path = FRONTEND_DIST / full_path
        if full_path and file_path.exists() and file_path.is_file():
            return FileResponse(str(file_path))
        # Otherwise serve index.html for client-side routing
        return FileResponse(str(FRONTEND_DIST / "index.html"))

    print(f"[Startup] Serving React frontend from {FRONTEND_DIST}")
else:
    print(f"[Startup] No frontend build found at {FRONTEND_DIST}. Run 'cd frontend && npm run build' to enable.")

