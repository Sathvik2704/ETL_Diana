import os
import requests
import google.generativeai as genai

_OLLAMA_URL = "http://localhost:11434/api/generate"
_MODEL_CANDIDATES = ["llama3", "mistral"]
_OLLAMA_TIMEOUT_S = 20

# Setup Gemini API key
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "AIzaSyCGtbmCUh9YPcVeKDaGRQmumfQ3C9vbEnY")
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)

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

def _canonical_prompt(goal: str, columns: list[str], value_hints: dict[str, list[str]] | None, error_msg: str | None = None) -> str:
    cols_repr = repr(columns)
    hints_repr = repr(value_hints or {})
    
    prompt = f"""
You are an expert data engineer writing Pandas ETL code.

Dataset columns (exact, case-sensitive): {cols_repr}
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
- Assume `df` and `pd` are already defined.
- You have full access to advanced data science libraries. IMPORT THEM at the top of your code if needed (e.g., `from sklearn.impute import KNNImputer`, `from scipy import stats`, `import numpy as np`).
- Use column names EXACTLY as provided (case-sensitive).
- Mutate `df` in place or reassign `df`. Do NOT return a new variable, store the final output in `df`.
- Do NOT print anything.
- Missing values & Outlier policy (critical):
  - Do not use basic `.fillna()` if a mathematically superior technique is requested/appropriate. 
  - Use `sklearn.impute.KNNImputer`, `SimpleImputer`, or `LinearRegression` for numeric imputation.
  - Use mathematical outlier filtering like `scipy.stats.zscore` (Z-score > 3) or `sklearn.ensemble.IsolationForest` when handling anomalies.
- Advanced filtering and strings:
  - When filtering string values, aggressively normalize case (e.g. `df[df["Status"].astype(str).str.strip().str.upper() == "FAILED"]`).
- Ensure the final result is strictly stored in the variable `df`.
"""
    return prompt.strip()

def _call_gemini(prompt: str) -> str:
    # Use gemini-1.5-pro
    model = genai.GenerativeModel('gemini-1.5-pro')
    response = model.generate_content(prompt)
    return response.text

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
    error_msg: str | None = None,
) -> str:
    """
    Generates pandas transformation code based on goal.
    Uses Google AI Studio (Gemini) if internet is available, otherwise uses local Ollama.
    """
    prompt = _canonical_prompt(goal, columns, value_hints, error_msg)
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
