# Power BI Integration

Power BI can connect to Gold Delta tables using the **Azure Databricks connector** or
by reading Parquet files directly from the Delta Lake directory.

## Connection Options

### Option 1: Azure Databricks Connector
1. Open Power BI Desktop
2. Get Data → Azure → Azure Databricks
3. Enter your Databricks workspace URL
4. Connect to the Gold Delta tables via SQL

### Option 2: Direct Parquet (local/on-prem)
1. Open Power BI Desktop
2. Get Data → File → Parquet
3. Navigate to the `_delta_log/` directory and read the latest snapshot files
4. Alternatively, use Python/Pandas to export Delta to CSV first

### Option 3: Python Script Connector
1. Get Data → Other → Python script
2. Use the `deltalake` Python library to load Gold tables:
```python
from deltalake import DeltaTable
import pandas as pd

dt = DeltaTable("/tmp/cricket-delta/gold/batch_kpis/top_run_scorers")
df = dt.to_pandas()
```

## Recommended Dashboards

- **Player Performance Dashboard**: Top batsmen/bowlers, strike rates, averages
- **Match Analysis Dashboard**: Head-to-head, venue stats, toss impact
- **Trend Analysis Dashboard**: Run rate progression, pressure index over overs
- **Live Match Dashboard**: Connect to `gold/live_kpis` for real-time updates

## Refresh Schedule
For live data: set Power BI refresh to every 5 minutes.
For batch KPIs: refresh daily after the batch pipeline runs.
