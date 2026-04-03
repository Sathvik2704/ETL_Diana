import os
import requests
import time
import json
from pathlib import Path

# Securely load environment variables from .env
# Securely load environment variables from .env if present
env_path = Path(__file__).parent / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ[key.strip()] = val.strip()

_OLLAMA_URL = "http://localhost:11434/api/generate"
_MODEL_CANDIDATES = ["llama3", "mistral"]
_OLLAMA_TIMEOUT_S = 20

# Setup Gemini API key
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

def _has_internet_connection() -> bool:
    try:
        # Try to request a highly reliable endpoint
        requests.get("https://8.8.8.8", timeout=2)
        return True
    except requests.exceptions.RequestException:
        pass
    try:
        requests.get("https://google.com", timeout=2)
        return True
    except requests.exceptions.RequestException:
        return False

def _canonical_prompt(goal: str, columns: list[str], value_hints: dict[str, list[str]] | None, error_msg: str | None = None, dtypes: dict[str, str] | None = None) -> str:
    cols_repr = repr(columns)
    hints_repr = repr(value_hints or {})
    dtypes_repr = repr(dtypes or {})
    
    prompt = f"""
You are an expert data engineer writing Pandas ETL code.

Dataset columns (exact, case-sensitive): {cols_repr}
Column data types: {dtypes_repr}
Known example values by column (use these for exact matching when filtering): {hints_repr}
User goal: {goal}
"""
    if error_msg:
        prompt += f"""
PREVIOUS CODE EXECUTION FAILED WITH ERROR:
{error_msg}

Please analyze the error and fix your previously generated code to resolve it. Give only the corrected python code.
"""

    prompt += """
Write ONLY valid Python code that operates on an existing pandas DataFrame named `df`.
Rules:
- Output ONLY code. No explanations, no comments, no markdown, no backticks.
- `df`, `pd` (pandas), and `np` (numpy) are already defined. Do NOT import pandas or numpy.
- You CAN import other libraries like sklearn, scipy etc. at the top of your code if needed.
- Use column names EXACTLY as provided (case-sensitive).
- Store the final result in `df`. Mutate in place or reassign.
- Do NOT call print().

MISSING VALUE IMPUTATION (CRITICAL — follow these rules exactly):
When the user asks to "fill missing values" or "impute":
1. For NUMERIC columns (int64, float64):
   - Default: use `df[col].fillna(df[col].median())` — this always works.
   - If user mentions "mean": use `df[col].fillna(df[col].mean())`
   - If user mentions "KNN" or "regression": use sklearn (e.g. KNNImputer), but handle mixed types carefully.
2. For TEXT/OBJECT columns (object, string):
   - Use mode: `df[col].fillna(df[col].mode().iloc[0])` if mode is non-empty.
   - If mode is empty, use `df[col].fillna("Unknown")`.
3. If user says "fill missing values" without specifying columns, fill ALL columns.
4. If user specifies a column (e.g. "fill missing values for math column"), fill ONLY that column.
5. NEVER leave missing values unfilled when the user explicitly asks to fill them.

OUTLIER REMOVAL:
- Default: IQR method. Remove rows where value < Q1–1.5*IQR or > Q3+1.5*IQR.
- If user says "z-score": use `scipy.stats.zscore` and filter |z| > 3.
- If user says "isolation forest": use `sklearn.ensemble.IsolationForest`.

DUPLICATE REMOVAL:
- Use `df.drop_duplicates()` and reset index.

STRING FILTERING:
- Always normalize: `df[col].astype(str).str.strip().str.upper()` before comparing.

Ensure the final result is strictly stored in the variable `df`.
"""
    return prompt.strip()

def _call_gemini(prompt: str) -> str:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}"
    payload = {
        "contents": [{"parts":[{"text": prompt}]}]
    }
    
    # Improved retry loop for 429/500
    last_error = None
    for i in range(3):
        try:
            time.sleep(1) # Small delay to avoid burst
            r = requests.post(url, json=payload, timeout=20)
            if r.status_code == 429:
                last_error = Exception(f"Rate limited (429) on attempt {i+1}/3")
                time.sleep(4)
                continue
            r.raise_for_status()
            data = r.json()
            return data["candidates"][0]["content"]["parts"][0]["text"]
        except Exception as e:
            last_error = e
            time.sleep(2)
    raise Exception(f"Gemini API failed after retries. Last error: {last_error}")

def _canonical_call(model: str, prompt: str) -> str:
    r = requests.post(
        _OLLAMA_URL,
        json={
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0},
        },
        timeout=_OLLAMA_TIMEOUT_S,
    )
    r.raise_for_status()
    return (r.json().get("response") or "").strip()

def _canonical_strip(code: str) -> str:
    code = code.strip()
    if not code.startswith("```"):
        return code
    parts = code.split("```")
    inner_sections = [p for p in parts[1:] if p.strip()]
    if not inner_sections:
        return code.strip("`").strip()
    body = inner_sections[0]
    lines = body.splitlines()
    if lines and (lines[0].strip().lower().startswith("python") or lines[0].strip().lower() == "python"):
        lines = lines[1:]
    return "\n".join(lines).strip()

def generate_transformation(
    goal: str,
    columns: list[str],
    *,
    value_hints: dict[str, list[str]] | None = None,
    dtypes: dict[str, str] | None = None,
    error_msg: str | None = None,
) -> str:
    """
    Generates pandas transformation code based on goal.
    Uses Google AI Studio (Gemini) if internet is available, otherwise uses local Ollama.
    """
    prompt = _canonical_prompt(goal, columns, value_hints, error_msg, dtypes=dtypes)
    last_err: Exception | None = None
    code = ""
    
    if _has_internet_connection():
        print("Using Google AI Studio (Gemini)...")
        try:
            code = _call_gemini(prompt)
        except Exception as exc:
            print(f"Gemini call failed: {exc}. Falling back to Ollama.")
            last_err = exc
            code = ""
            
    if not code:
        print("Using local Ollama...")
        for model in _MODEL_CANDIDATES:
            try:
                code = _canonical_call(model, prompt)
                if code:
                    break
            except Exception as exc:
                last_err = exc

    if not code and last_err:
        raise RuntimeError(
            "Failed to generate transformation code via Gemini and Ollama. "
            "Ensure internet connection or local Ollama is running."
        ) from last_err

    return _canonical_strip(code)
