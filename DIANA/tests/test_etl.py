import pandas as pd

import agents.coder
from etl import run_etl


def test_missing_values_removed(tmp_path, monkeypatch):
    """
    Basic automation proof:
    - Create a CSV with missing values
    - Ask the system to remove missing values
    - Check that output CSV has no nulls
    """
    input_path = tmp_path / "test.csv"
    df = pd.DataFrame({"A": [1, None, 3]})
    df.to_csv(input_path, index=False)

    # Avoid calling a real LLM in unit tests.
    monkeypatch.setattr(agents.coder.CoderAgent, "generate_code", lambda *args, **kwargs: "df = df.dropna()")

    output_path = run_etl(str(input_path), "remove missing values")

    result = pd.read_csv(output_path)
    assert result.isnull().sum().sum() == 0

