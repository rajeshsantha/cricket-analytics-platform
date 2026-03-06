"""Export all Gold KPI Delta tables to Parquet files for Streamlit Cloud deployment."""
import os
from deltalake import DeltaTable

delta_base = "/tmp/cricket-delta/gold/batch_kpis"
out_base = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(out_base, exist_ok=True)

for kpi in sorted(os.listdir(delta_base)):
    src = os.path.join(delta_base, kpi)
    if not os.path.isdir(src):
        continue
    try:
        df = DeltaTable(src).to_pandas()
        out = os.path.join(out_base, kpi + ".parquet")
        df.to_parquet(out, index=False)
        print(f"  OK  {kpi}: {len(df)} rows -> {out}")
    except Exception as e:
        print(f"  FAIL {kpi}: {e}")

print("Export complete.")

