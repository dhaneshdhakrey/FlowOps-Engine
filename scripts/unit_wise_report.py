import os
import json
from datetime import datetime, timedelta
from pyhive import hive
from logging_config1 import start_logging

logger = start_logging("unit_wise_report_job.log")

def fetch_unit_wise_report(target_date: str):
    """
    Fetch unit_wise_report data from Hive for a given date.
    Returns standardized dict format.
    """
    try:
        logger.info(f"Unit Wise Report ETL started for {target_date}")

        # Paths
        folder_path = f"/home/hadoop/dhanesh/au_360/au_analytics/unit_wise_report/{target_date}"
        os.makedirs(folder_path, exist_ok=True)
        file_path = os.path.join(folder_path, f"response_{target_date}.json")

        # Query
        query = f"""
            SELECT * 
            FROM au_360.unit_vise_report 
            WHERE report_date = '{target_date}'
        """
        logger.info(f"Executing query for {target_date}")

        # Hive connection
        conn = hive.Connection(
            host="10.59.***", port=10000, username="username", database="au_360"
        )
        cursor = conn.cursor()
        cursor.execute(query)
        api_data = cursor.fetchall()

        column_names = [
            "story_unit",
            "story_city",
            "story_count",
            "video_count",
            "no_videos",
            "no_media",
            "users",
            "views",
            "report_date"
        ]

        # Stats
        total_rows = len(api_data)
        total_columns = len(column_names) if total_rows > 0 else 0
        total_doc = total_rows * total_columns if total_rows > 0 else 0

        if total_rows == 0:
            if os.path.exists(file_path):
                logger.warning(f"No new data for {target_date}. Existing file present → skipping overwrite.")
                status = "no_data_skipped"
            else:
                logger.warning(f"No data found for {target_date}. Creating empty file.")
                with open(file_path, "w") as jf:
                    json.dump([], jf, indent=4)
                status = "no_data"
        else:
            # Save data
            data_to_write = [dict(zip(column_names, row)) for row in api_data]
            with open(file_path, "w") as jf:
                json.dump(data_to_write, jf, indent=4)
            logger.info(f"Data saved successfully to {file_path}")
            status = "success"

        cursor.close()
        conn.close()

        return {
            "status": status,
            "total_rows": total_rows,
            "total_columns": total_columns,
            "total_doc": total_doc,
            "saved_file": file_path if os.path.exists(file_path) else None
        }

    except Exception as e:
        logger.error(f"Unit Wise Report ETL failed for {target_date}: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "total_rows": 0,
            "total_columns": 0,
            "total_doc": 0,
            "saved_file": None
        }

def process_unit_wise_report():
    """
    Runs ETL for current day and previous day.
    Returns a single summary dictionary.
    """
    cur_date = datetime.now().strftime("%Y-%m-%d")
    prev_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    current_day = fetch_unit_wise_report(cur_date)
    previous_day = fetch_unit_wise_report(prev_date)

    # Aggregate totals
    total_doc = current_day["total_doc"] + previous_day["total_doc"]
    total_rows = current_day["total_rows"] + previous_day["total_rows"]

    if total_doc > 0:
        return {
            "status": "success",
            "total_rows": total_rows,
            "total_columns": max(current_day["total_columns"], previous_day["total_columns"]),
            "total_doc": total_doc,
            "current_day_file": current_day["saved_file"],
            "previous_day_file": previous_day["saved_file"]
        }
    else:
        return {
            "status": "no_data",
            "total_rows": 0,
            "total_columns": 0,
            "total_doc": 0,
            "current_day_file": current_day["saved_file"],
            "previous_day_file": previous_day["saved_file"]
        }


if __name__ == "__main__":
    
    if len(sys.argv) > 1:
        date_arg = sys.argv[1]
    else:
        date_arg = datetime.today().strftime("%Y-%m-%d")

    ans = process_unit_wise_report() 
    print(ans)