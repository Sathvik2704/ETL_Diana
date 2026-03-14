from etl import run_etl
import pandas as pd

try:
    print("Testing ETL with Gemini...")
    out_path = run_etl("test_data.csv", "Fill missing ages with average age")
    print(f"Success! Output saved to: {out_path}")
    df = pd.read_csv(out_path)
    print("Final DataFrame:")
    print(df)
except Exception as e:
    print(f"ETL failed with: {e}")
