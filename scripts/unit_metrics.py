import os
import json
import pandas as pd
from pyhive import hive
from datetime import datetime
from logging_config1 import start_logging

logger = start_logging("overall_unit_metrics.log")

OUTPUT_ROOT = "/home/hadoop/dhanesh/au_360/au_analytics/"


def save_dataframe(df, folder, filename="response.json"):
    """
    Save dataframe to JSON with overwrite-checking logic.
    """
    os.makedirs(folder, exist_ok=True)
    file_path = os.path.join(folder, filename)

    total_rows = len(df)
    total_columns = len(df.columns) if total_rows > 0 else 0
    total_doc = total_rows * total_columns if total_rows > 0 else 0

    if total_rows == 0:
        if os.path.exists(file_path):
            logger.warning(f"No new data found → Skipping overwrite. Existing file: {file_path}")
            status = "no_data_skipped"
        else:
            logger.warning(f"No data found → Creating empty file at {file_path}")
            df.to_json(file_path, orient="records", lines=True)
            status = "no_data"
    else:
        df.to_json(file_path, orient="records", lines=True)
        logger.info(f"Saved {total_rows} rows to {file_path}")
        status = "success"

    return {
        "status": status,
        "total_rows": total_rows,
        "total_columns": total_columns,
        "total_doc": total_doc,
        "saved_file": file_path if os.path.exists(file_path) else None,
    }


def fetch_overall_unit_metrics():
    """
    Fetch and save Overall Unit Metrics (lastN, daily, monthly).
    """
    results = []

    try:
        conn = hive.Connection(host="10.59.***", port=10000, database="au_360")

        # --- lastN periods ---
        lastN_values = ["7days", "15days", "30days", "60days"]
        for lastN in lastN_values:
            query = f"SELECT users FROM unit_lastN_metrics WHERE lastN = '{lastN}'"
            df = pd.read_sql(query, conn)
            folder = os.path.join(OUTPUT_ROOT, "overall_unit_lastN_metrics", f"lastN={lastN}")
            result = save_dataframe(df, folder)
            result["metric_type"] = f"lastN_{lastN}"
            results.append(result)

        # --- daily metrics ---
        today = datetime.today().strftime("%Y-%m-%d")
        query = f"SELECT users FROM unit_daily_metrics WHERE report_date = '{today}'"
        df = pd.read_sql(query, conn)
        folder = os.path.join(OUTPUT_ROOT, "overall_unit_daily_metrics", f"report_date={today}")
        result = save_dataframe(df, folder)
        result["metric_type"] = "daily"
        results.append(result)

        # --- monthly metrics ---
        current_month = datetime.today().strftime("%B")
        query = f"SELECT users FROM unit_monthly_metrics WHERE month = '{current_month}'"
        df = pd.read_sql(query, conn)
        folder = os.path.join(OUTPUT_ROOT, "overall_unit_monthly_metrics", f"month={current_month}")
        result = save_dataframe(df, folder)
        result["metric_type"] = "monthly"
        results.append(result)

        conn.close()

    except Exception as e:
        logger.error(f"ETL failed: {str(e)}", exc_info=True)
        return [{
            "status": "error",
            "total_rows": 0,
            "total_columns": 0,
            "total_doc": 0,
            "saved_file": None,
            "metric_type": "all"
        }]
    total_rows = sum(r.get("total_rows", 0) for r in results)
    total_columns = sum(r.get("total_columns", 0) for r in results)
    total_doc = sum(r.get("total_doc", 0) for r in results)
    return {
        "status": "success" if any(r["status"] == "success" for r in results) else "no_data",
        "total_rows": total_rows,
        "total_columns": total_columns,
        "total_doc": total_doc,
        "saved_file": None  # multiple files created → use None, or switch to list if you want all file paths
    }
    
if __name__ == "__main__":
    
    if len(sys.argv) > 1:
        date_arg = sys.argv[1]
    else:
        date_arg = datetime.today().strftime("%Y-%m-%d")

    ans = fetch_overall_unit_metrics() 
    print(ans)
