import os
import json
import pandas as pd
from pyhive import hive
from datetime import datetime
from logging_config1 import start_logging

logger = start_logging("au_partners_job.log")


def process_au_partners(target_date: str):
    """
    Fetch and merge AU Partners data for a given date.
    Returns standardized dict format.
    """
    try:
        logger.info(f"AU Partners ETL started for {target_date}")

        # Paths
        folder_path = f"/home/hadoop/dhanesh/au_360/au_analytics/AU_partners/{target_date}"
        os.makedirs(folder_path, exist_ok=True)
        file_path = os.path.join(folder_path, f"response_{target_date}.json")

        # Queries
        queries = {
            "readtime": f"SELECT partners_title, readtime FROM au_360.auw_gen_partners_readtime WHERE report_date='{target_date}'",
            "stories": f"SELECT partners_title, stories FROM au_360.auw_gen_partners_stories WHERE report_date='{target_date}'",
            "users": f"SELECT partners_title, users FROM au_360.auw_gen_partners_users WHERE report_date='{target_date}'",
            "views": f"SELECT partners_title, views FROM au_360.auw_gen_partners_views WHERE report_date='{target_date}'",
        }

        # Hive connection
        conn = hive.Connection(
            host="10.59.***", port=10000, username="username", database="au_360"
        )
        cursor = conn.cursor()

        # Execute queries and build DataFrames
        dfs = {}
        for key, query in queries.items():
            logger.info(f"Executing {key} query for {target_date}")
            cursor.execute(query)
            data = cursor.fetchall()
            dfs[key] = (
                pd.DataFrame(data, columns=["partners_title", key])
                if data
                else pd.DataFrame(columns=["partners_title", key])
            )

        cursor.close()
        conn.close()

        # Merge DataFrames
        df_combined = dfs["stories"]
        for key in ["readtime", "users", "views"]:
            df_combined = df_combined.merge(dfs[key], on="partners_title", how="left")

        # Drop rows with missing partners_title
        df_combined = df_combined.dropna(subset=["partners_title"])

        # Stats
        total_rows = len(df_combined)
        total_columns = len(df_combined.columns) if total_rows > 0 else 0
        total_doc = total_rows * total_columns if total_rows > 0 else 0

        # Save or skip
        if total_rows == 0:
            if os.path.exists(file_path):
                logger.warning(
                    f"No new data for {target_date}. Existing file present → skipping overwrite."
                )
                status = "no_data_skipped"
            else:
                logger.warning(f"No data found for {target_date}. Creating empty file.")
                with open(file_path, "w") as jf:
                    json.dump([], jf, indent=4)
                status = "no_data"
        else:
            result_dict = df_combined.to_dict(orient="records")
            with open(file_path, "w") as jf:
                json.dump(result_dict, jf, indent=4)
            logger.info(f"Data saved successfully to {file_path}")
            status = "success"

        return {
            "status": status,
            "total_rows": total_rows,
            "total_columns": total_columns,
            "total_doc": total_doc,
            "saved_file": file_path if os.path.exists(file_path) else None,
        }

    except Exception as e:
        logger.error(f"AU Partners ETL failed for {target_date}: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "total_rows": 0,
            "total_columns": 0,
            "total_doc": 0,
            "saved_file": None,
        }


if __name__ == "__main__":
    
    if len(sys.argv) > 1:
        date_arg = sys.argv[1]
    else:
        date_arg = datetime.today().strftime("%Y-%m-%d")

    ans = process_au_partners(date_arg) 
    print(ans)
