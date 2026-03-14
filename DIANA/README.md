# DIANA – Minimal Autonomous ETL Prototype

This is a **Minimum Viable DIANA** implementation:

- Upload one CSV
- Provide a natural-language goal
- Local LLM (Ollama: LLaMA 3 / Mistral) generates a Pandas transformation
- Code is executed
- Output CSV is saved
- Pytest verifies the pipeline end-to-end

---

## 1. Project Structure

- `main.py` – FastAPI app (backend + HTML landing route)
- `llm.py` – LLM helper (simulated Planner + Coder agent)
- `etl.py` – ETL runner (Executor + simple QA via error handling)
- `index.html` – Minimal frontend (file upload + goal)
- `tests/test_etl.py` – Pytest to prove automation
- `uploads/` – Where uploaded CSVs are stored

---

## 2. Setup

From the `DIANA/` directory:

```bash
pip install -r requirements.txt
```

Install Ollama and pull a free local model:

```bash
ollama --version
ollama pull llama3
# (optional, lighter/faster)
# ollama pull mistral
```

Start the local LLM server (keep it running):

```bash
ollama serve
```

---

## 3. Run the Backend

From the `DIANA/` directory:

```bash
uvicorn main:app --reload
```

Then open in your browser:

- `http://127.0.0.1:8000/`

You will see:

1. File input for a CSV
2. Text box for the natural-language goal
3. Button to **Run ETL**

The API responds with the path of the generated `output.csv`.

---

## 4. Run Tests

From the `DIANA/` directory:

```bash
pytest
```

The main test:

- Creates a small CSV with missing values
- Calls `run_etl(..., "remove missing values")`
- Asserts that the resulting `output.csv` has no nulls

> Note: Tests do NOT call a real LLM (they mock the LLM call) so they run fast and offline.

---

## 5. How This Maps to the DIANA Architecture

- **User Interaction Layer**: `index.html` + `GET /` in `main.py`
- **Backend & Orchestration**: `FastAPI` app in `main.py`
- **Planner + Coder Agent**: `generate_transformation()` in `llm.py`
- **Executor Agent**: `run_etl()` in `etl.py`
- **QA Agent (simplified)**: error handling + tests
- **Storage & Output**: CSV files in `uploads/` (input) and `output.csv` (result)

This prototype is intentionally minimal but fully functional and aligned with the original DIANA proposal.

