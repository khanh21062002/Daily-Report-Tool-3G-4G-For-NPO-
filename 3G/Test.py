import pandas as pd

# Test đọc file CSV
df = pd.read_csv('RTWP_3G_Ericsson.csv', nrows=5)
print("Columns:", df.columns.tolist())
print("\nFirst column dtype:", df.iloc[:, 0].dtype)
print("\nFirst column values:")
print(df.iloc[:, 0])

# Thử parse dates
date_col = df.columns[0]
test_dates = pd.to_datetime(df[date_col], errors='coerce')
print(f"\nParsed dates successful: {test_dates.notna().sum()} out of {len(test_dates)}")