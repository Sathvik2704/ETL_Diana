import pandas as pd
from etl import run_etl

df = pd.DataFrame({
    'id': [1, 2, 3, 4, 5, 6, 7],
    'age': [21, 22, 100, 24, None, 22, 21],
    'score': [85, 90, 88, 10, None, 95, 96]
})
df.to_csv("test_advanced.csv", index=False)

print("Running Advanced ETL test...")
goal = "Remove outlier ages > 90 and score < 20 using scipy zscore or mathematical logical filtering. Then fill missing scores using KNNImputer from sklearn."

out_file = run_etl("test_advanced.csv", goal)
print("Finished saving output to:", out_file)

print("\nFinal Data:")
print(pd.read_csv(out_file))
