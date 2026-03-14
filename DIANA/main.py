from pathlib import Path

import pandas as pd  # noqa: F401  # imported to highlight dependency in this file
from fastapi import FastAPI, Form, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from etl import run_etl
from cleaning import CleanConfig, clean_dataframe


BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"

app = FastAPI(title="DIANA – Minimal ETL Prototype")

# Allow a React dev server (and other local tools) to call this API.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:5174",
        "http://127.0.0.1:5174",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def _safe_filename(name: str) -> str:
    # Avoid path traversal / weird filenames; keep it simple for an academic prototype.
    cleaned = Path(name).name
    if not cleaned or cleaned in {".", ".."}:
        raise HTTPException(status_code=400, detail="Invalid filename")
    return cleaned


def _upload_path(filename: str) -> Path:
    return UPLOAD_DIR / _safe_filename(filename)


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    """
    Simple HTML page to upload a CSV and specify a goal.
    """
    html_path = BASE_DIR / "index.html"
    if html_path.exists():
        return html_path.read_text(encoding="utf-8")

    # Fallback inline HTML if file is missing.
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>DIANA ETL Prototype</title>
    </head>
    <body>
        <h1>DIANA – Minimal ETL Prototype</h1>
        <form action="/process" method="post" enctype="multipart/form-data">
            <label>CSV File: <input type="file" name="file" accept=".csv" required></label><br/><br/>
            <label>Goal: <input type="text" name="goal" placeholder="e.g. remove missing values" required></label><br/><br/>
            <button type="submit">Run ETL</button>
        </form>
    </body>
    </html>
    """


@app.post("/process")
async def process(
    file: UploadFile = File(...),
    goal: str = Form(...),
) -> JSONResponse:
    """
    Accepts a CSV upload and a natural-language goal, then runs the ETL pipeline.
    """
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    input_path = _upload_path(file.filename)

    contents = await file.read()
    with open(input_path, "wb") as f:
        f.write(contents)

    output_file = run_etl(str(input_path), goal)
    output_filename = Path(output_file).name

    artifacts: dict[str, str] = {}
    warnings: list[str] = []

    # If the user asks for visualization, create charts and a rich HTML report
    # for the ETL output. These are stored as separate HTML files (non-CSV).
    goal_l = goal.lower()
    if any(
        k in goal_l
        for k in (
            "visual",
            "chart",
            "plot",
            "graph",
            "dashboard",
            "pie",
            "line",
            "time series",
            "area",
            "bar",
            "column",
            "scatter",
            "bubble",
        )
    ):
        try:
            df_out = pd.read_csv(output_file)
            if df_out.empty:
                warnings.append("ETL output is empty; charts were skipped.")
                return JSONResponse(
                    {
                        "output_file": output_file,
                        "output_filename": output_filename,
                        "download_url": f"/download/{output_filename}",
                        "artifacts": artifacts,
                        "warnings": warnings,
                    }
                )

            # Decide chart types based on goal keywords.
            wants_any = any(k in goal_l for k in ("visual", "chart", "plot", "graph", "dashboard"))
            wants_pie = "pie" in goal_l
            wants_line = "line" in goal_l or "time series" in goal_l
            wants_area = "area" in goal_l
            wants_bar = "bar" in goal_l
            wants_column = "column" in goal_l
            wants_scatter = "scatter" in goal_l
            wants_bubble = "bubble" in goal_l

            # If user says generic "chart/visual", generate all basic chart types.
            if wants_any and not any(
                [wants_pie, wants_line, wants_area, wants_bar, wants_column, wants_scatter, wants_bubble]
            ):
                wants_pie = wants_line = wants_area = wants_bar = wants_column = wants_scatter = wants_bubble = True
            # If the goal strongly implies proportions, default to a pie chart.
            if not wants_pie and any(k in goal_l for k in ("distribution", "proportion", "percentage", "share", "breakdown", "composition")):
                wants_pie = True

            # Heuristically pick a status-like column for pie chart.
            status_candidates = ["status", "delivery_status", "message_status", "state"]
            status_col = None
            for c in df_out.columns:
                cname = str(c).strip().lower()
                if cname in status_candidates:
                    status_col = c
                    break

            # Heuristically pick a time column + value column for line chart.
            time_col = None
            for c in df_out.columns:
                cname = str(c).strip().lower()
                if any(k in cname for k in ("date", "time", "timestamp", "created_at", "sent_at")):
                    time_col = c
                    break
            if time_col is None:
                # Fallback: first datetime-like column if any.
                for c in df_out.columns:
                    if pd.api.types.is_datetime64_any_dtype(df_out[c]):
                        time_col = c
                        break

            numeric_cols = [c for c in df_out.columns if pd.api.types.is_numeric_dtype(df_out[c])]
            categorical_cols = [
                c
                for c in df_out.columns
                if not pd.api.types.is_numeric_dtype(df_out[c])
                and not pd.api.types.is_datetime64_any_dtype(df_out[c])
            ]
            value_col = numeric_cols[0] if numeric_cols else None

            # Use a per-run suffix to avoid overwriting artifacts when the same
            # filename is uploaded multiple times.
            import uuid

            run_tag = uuid.uuid4().hex[:8]
            stem = f"{Path(output_filename).stem}_{run_tag}"

            def _pick_pie_source_column() -> str | None:
                if status_col is not None:
                    return str(status_col)
                if not categorical_cols:
                    return None
                best = None
                best_score = None
                for c in categorical_cols:
                    s = df_out[c]
                    nunq = int(s.astype("string").str.strip().replace({"": None}).nunique(dropna=True))
                    # Prefer low-cardinality columns; ignore columns that are essentially unique IDs.
                    if nunq <= 1:
                        continue
                    score = nunq
                    if best_score is None or score < best_score:
                        best_score = score
                        best = c
                return str(best) if best is not None else str(categorical_cols[0])

            # Pie chart first when requested so the UI embeds it as primary media.
            if wants_pie:
                try:
                    import plotly.express as px

                    pie_src = _pick_pie_source_column()
                    if pie_src is not None:
                        s = (
                            df_out[pie_src]
                            .astype("string")
                            .str.strip()
                            .replace({"": None})
                            .fillna("missing")
                            .str.lower()
                        )
                        counts = (
                            s.value_counts(dropna=False)
                            .reset_index()
                            .rename(columns={"index": pie_src, "count": "count"})
                        )
                        counts.columns = [pie_src, "count"]
                        fig = px.pie(
                            counts,
                            names=pie_src,
                            values="count",
                            title=f"Distribution of {pie_src}",
                        )
                        pie_path = _upload_path(f"etl_pie_{stem}.html")
                        fig.write_html(pie_path, include_plotlyjs="cdn", full_html=True)
                        artifacts["pie_chart_html"] = f"/download/{pie_path.name}"
                    elif value_col is not None:
                        # Numeric fallback: bin into quantiles and plot distribution.
                        s_num = pd.to_numeric(df_out[value_col], errors="coerce")
                        s_num = s_num.dropna()
                        if s_num.empty:
                            raise ValueError("No numeric data available for pie chart fallback")
                        bins = pd.qcut(s_num, q=min(4, s_num.nunique()), duplicates="drop")
                        counts = bins.astype("string").value_counts(dropna=False).reset_index()
                        counts.columns = ["bin", "count"]
                        fig = px.pie(
                            counts,
                            names="bin",
                            values="count",
                            title=f"Distribution of binned {value_col}",
                        )
                        pie_path = _upload_path(f"etl_pie_{stem}.html")
                        fig.write_html(pie_path, include_plotlyjs="cdn", full_html=True)
                        artifacts["pie_chart_html"] = f"/download/{pie_path.name}"
                    else:
                        warnings.append("Pie chart requested, but no suitable categorical or numeric column was found.")
                except Exception as exc:  # noqa: BLE001
                    warnings.append(f"Failed to generate pie chart: {exc}")

            # Line chart: by time (x) and count or first numeric (y).
            if wants_line and time_col is not None:
                try:
                    import plotly.express as px

                    df_line = df_out.copy()
                    df_line[time_col] = pd.to_datetime(df_line[time_col], errors="coerce")
                    df_line = df_line.dropna(subset=[time_col])
                    if df_line.empty:
                        raise ValueError("No valid datetime values for line chart")

                    if value_col is None:
                        # Count of rows per time.
                        grouped = df_line.groupby(time_col).size().reset_index(name="count")
                        y_col = "count"
                    else:
                        grouped = (
                            df_line.groupby(time_col)[value_col]
                            .sum()
                            .reset_index()
                        )
                        y_col = value_col

                    fig = px.line(
                        grouped,
                        x=time_col,
                        y=y_col,
                        title=f"Time series of {y_col}",
                    )
                    line_path = _upload_path(f"etl_line_{stem}.html")
                    fig.write_html(line_path, include_plotlyjs="cdn", full_html=True)
                    artifacts["line_chart_html"] = f"/download/{line_path.name}"
                except Exception as exc:  # noqa: BLE001
                    warnings.append(f"Failed to generate line chart: {exc}")
            elif wants_line and time_col is None and value_col is not None:
                # Fallback: line over index vs first numeric column.
                try:
                    import plotly.express as px

                    df_line = df_out.reset_index().rename(columns={"index": "row"})
                    fig = px.line(
                        df_line,
                        x="row",
                        y=value_col,
                        title=f"{value_col} over rows",
                    )
                    line_path = _upload_path(f"etl_line_{stem}.html")
                    fig.write_html(line_path, include_plotlyjs="cdn", full_html=True)
                    artifacts["line_chart_html"] = f"/download/{line_path.name}"
                except Exception as exc:  # noqa: BLE001
                    warnings.append(f"Failed to generate line chart: {exc}")

            # Area chart (mirrors line chart logic).
            if wants_area and value_col is not None:
                try:
                    import plotly.express as px

                    if time_col is not None:
                        df_area = df_out.copy()
                        df_area[time_col] = pd.to_datetime(df_area[time_col], errors="coerce")
                        df_area = df_area.dropna(subset=[time_col])
                        if df_area.empty:
                            raise ValueError("No valid datetime values for area chart")
                        grouped = df_area.groupby(time_col)[value_col].sum().reset_index()
                        x_col = time_col
                    else:
                        df_area = df_out.reset_index().rename(columns={"index": "row"})
                        grouped = df_area
                        x_col = "row"

                    fig = px.area(
                        grouped,
                        x=x_col,
                        y=value_col,
                        title=f"Area chart of {value_col}",
                    )
                    area_path = _upload_path(f"etl_area_{stem}.html")
                    fig.write_html(area_path, include_plotlyjs="cdn", full_html=True)
                    artifacts["area_chart_html"] = f"/download/{area_path.name}"
                except Exception as exc:  # noqa: BLE001
                    warnings.append(f"Failed to generate area chart: {exc}")

            # Bar / column chart: category vs numeric (or counts).
            if (wants_bar or wants_column) and (categorical_cols or value_col is not None):
                try:
                    import plotly.express as px

                    cat_col = categorical_cols[0] if categorical_cols else None
                    if cat_col is not None and value_col is not None:
                        grouped = (
                            df_out.groupby(cat_col)[value_col]
                            .sum()
                            .reset_index()
                        )
                        y_col = value_col
                    elif cat_col is not None:
                        grouped = df_out[cat_col].value_counts().reset_index()
                        grouped.columns = [cat_col, "count"]
                        y_col = "count"
                    else:
                        # No categorical cols: use index buckets.
                        grouped = (
                            df_out.reset_index()
                            .assign(bucket=lambda d: d["index"] // 10)
                            .groupby("bucket")
                            .size()
                            .reset_index(name="count")
                        )
                        cat_col = "bucket"
                        y_col = "count"

                    fig_bar = px.bar(
                        grouped,
                        x=cat_col,
                        y=y_col,
                        title=f"Bar chart of {y_col} by {cat_col}",
                    )
                    bar_path = _upload_path(f"etl_bar_{stem}.html")
                    fig_bar.write_html(bar_path, include_plotlyjs="cdn", full_html=True)
                    artifacts["bar_chart_html"] = f"/download/{bar_path.name}"

                    fig_col = px.bar(
                        grouped,
                        x=cat_col,
                        y=y_col,
                        title=f"Column chart of {y_col} by {cat_col}",
                    )
                    col_path = _upload_path(f"etl_column_{stem}.html")
                    fig_col.write_html(col_path, include_plotlyjs="cdn", full_html=True)
                    artifacts["column_chart_html"] = f"/download/{col_path.name}"
                except Exception as exc:  # noqa: BLE001
                    warnings.append(f"Failed to generate bar/column charts: {exc}")

            # Scatter / bubble plots: numeric vs numeric, optional size.
            if (wants_scatter or wants_bubble) and len(numeric_cols) >= 2:
                try:
                    import plotly.express as px

                    x_col = numeric_cols[0]
                    y_col = numeric_cols[1]
                    color_col = categorical_cols[0] if categorical_cols else None

                    fig_scatter = px.scatter(
                        df_out,
                        x=x_col,
                        y=y_col,
                        color=color_col,
                        title=f"Scatter plot of {y_col} vs {x_col}",
                    )
                    scat_path = _upload_path(f"etl_scatter_{stem}.html")
                    fig_scatter.write_html(scat_path, include_plotlyjs="cdn", full_html=True)
                    artifacts["scatter_chart_html"] = f"/download/{scat_path.name}"

                    if wants_bubble:
                        size_col = numeric_cols[2] if len(numeric_cols) >= 3 else None
                        fig_bubble = px.scatter(
                            df_out,
                            x=x_col,
                            y=y_col,
                            color=color_col,
                            size=size_col,
                            title=f"Bubble chart of {y_col} vs {x_col}",
                        )
                            # Note: if size_col is None, Plotly will ignore size argument.
                        bubble_path = _upload_path(f"etl_bubble_{stem}.html")
                        fig_bubble.write_html(bubble_path, include_plotlyjs="cdn", full_html=True)
                        artifacts["bubble_chart_html"] = f"/download/{bubble_path.name}"
                except Exception as exc:  # noqa: BLE001
                    warnings.append(f"Failed to generate scatter/bubble charts: {exc}")

            # Sweetviz overview report.
            try:
                import sweetviz as sv  # type: ignore[import-not-found]

                report = sv.analyze(df_out)
                viz_path = _upload_path(f"etl_visual_{stem}.html")
                report.show_html(filepath=str(viz_path), open_browser=False)
                artifacts["visualization_html"] = f"/download/{viz_path.name}"
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"Failed to generate profiling report: {exc}")
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"Visualization step failed: {exc}")

    return JSONResponse(
        {
            "output_file": output_file,
            "output_filename": output_filename,
            "download_url": f"/download/{output_filename}",
            "artifacts": artifacts,
            "warnings": warnings,
        }
    )


@app.post("/transform")
async def transform(
    file: UploadFile = File(...),
) -> JSONResponse:
    """
    Deterministic "raw -> clean" transform (no LLM required).
    Produces:
    - cleaned CSV
    - JSON QA report
    - (best-effort) missingness visuals + an HTML profiling report when optional libs are installed
    """
    import json
    import uuid

    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    run_id = uuid.uuid4().hex[:10]
    input_name = f"raw_{run_id}.csv"
    input_path = _upload_path(input_name)

    contents = await file.read()
    with open(input_path, "wb") as f:
        f.write(contents)

    try:
        df_raw = pd.read_csv(input_path)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"Failed to read CSV: {exc}") from exc

    df_clean, report = clean_dataframe(df_raw, config=CleanConfig())
    report["run_id"] = run_id

    cleaned_name = f"clean_{run_id}.csv"
    cleaned_path = _upload_path(cleaned_name)
    df_clean.to_csv(cleaned_path, index=False)

    report_name = f"clean_report_{run_id}.json"
    report_path = _upload_path(report_name)
    report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    artifacts: dict[str, str] = {}

    # To keep response time reasonable on large files, use a row sample for heavy visualizations.
    viz_sample = df_raw
    max_viz_rows = 5000
    if len(viz_sample) > max_viz_rows:
        viz_sample = viz_sample.sample(n=max_viz_rows, random_state=0)

    # Missingness plot (missingno) - best effort.
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        import missingno as msno

        fig = msno.matrix(viz_sample, figsize=(10, 4))
        fig_path = _upload_path(f"missingness_{run_id}.png")
        plt.savefig(fig_path, bbox_inches="tight", dpi=140)
        plt.close()
        artifacts["missingness_png"] = f"/download/{fig_path.name}"
    except Exception:  # noqa: BLE001
        pass

    # Quick interactive missingness bar (plotly) - best effort.
    try:
        import plotly.express as px

        miss = viz_sample.isna().mean().sort_values(ascending=False)
        miss_df = pd.DataFrame({"column": miss.index.astype(str), "missing_rate": miss.values})
        fig = px.bar(miss_df, x="column", y="missing_rate", title="Missingness by column")
        html_path = _upload_path(f"missingness_{run_id}.html")
        fig.write_html(html_path, include_plotlyjs="cdn", full_html=True)
        artifacts["missingness_html"] = f"/download/{html_path.name}"
    except Exception:  # noqa: BLE001
        pass

    # Full profiling report (ydata-profiling) - best effort, on a capped sample for speed.
    try:
        from ydata_profiling import ProfileReport

        prof_sample = df_clean
        max_prof_rows = 5000
        if len(prof_sample) > max_prof_rows:
            prof_sample = prof_sample.sample(n=max_prof_rows, random_state=0)
        profile = ProfileReport(prof_sample, minimal=True, explorative=True)
        profile_path = _upload_path(f"profile_{run_id}.html")
        profile.to_file(profile_path)
        artifacts["profile_html"] = f"/download/{profile_path.name}"
    except Exception:  # noqa: BLE001
        pass

    return JSONResponse(
        {
            "run_id": run_id,
            "input_file": str(input_path),
            "cleaned_file": str(cleaned_path),
            "download_cleaned_url": f"/download/{cleaned_name}",
            "download_report_url": f"/download/{report_name}",
            "artifacts": artifacts,
            "qa_summary": report.get("qa", {}),
        }
    )


@app.get("/download/{filename}")
async def download_output(filename: str) -> FileResponse:
    """
    Download a generated artifact (CSV/HTML/PNG/JSON).
    """
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
    }.get(ext, "application/octet-stream")

    disposition = "inline" if ext in {".html", ".htm", ".png"} else "attachment"
    return FileResponse(
        path,
        media_type=media_type,
        headers={"Content-Disposition": f'{disposition}; filename="{path.name}"'},
    )

