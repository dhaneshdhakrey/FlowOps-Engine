import os
import json
from pyhive import hive
from datetime import datetime, timedelta
from logging_config1 import start_logging

logger = start_logging("unit_report.log")


def fetch_unit_report(target_date: str):
    """
    Fetch and save Unit Report data for a specific date.
    Returns standardized dict format.
    """
    try:
        logger.info(f"Unit Report ETL started for {target_date}")

        folder_path = f"/home/hadoop/dhanesh/au_360/au_analytics/unit_report/{target_date}"
        os.makedirs(folder_path, exist_ok=True)
        file_path = os.path.join(folder_path, f"response_{target_date}.json")

        query = f"SELECT * FROM au_360.unit_report WHERE report_date='{target_date}'"

        conn = hive.Connection(
            host="10.59.***", port=10000, username="username", database="au_360"
        )
        cursor = conn.cursor()
        cursor.execute(query)
        api_data = cursor.fetchall()

        column_names = [
            "story_unit",
            "story_count",
            "video_count",
            "no_videos",
            "no_media",
            "views",
            "users",
            "report_date",
        ]

        data_to_write = [dict(zip(column_names, row)) for row in api_data]

        total_rows = len(data_to_write)
        total_columns = len(column_names) if total_rows > 0 else 0
        total_doc = total_rows * total_columns if total_rows > 0 else 0

        if total_rows == 0:
            if os.path.exists(file_path):
                logger.warning(
                    f"No new data for {target_date}. Existing file found → skipping overwrite."
                )
                status = "no_data_skipped"
            else:
                logger.warning(f"No data found for {target_date}. Creating empty file.")
                with open(file_path, "w") as jf:
                    json.dump([], jf, indent=4)
                status = "no_data"
        else:
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
            "saved_file": file_path if os.path.exists(file_path) else None,
        }

    except Exception as e:
        logger.error(f"Unit Report ETL failed for {target_date}: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "total_rows": 0,
            "total_columns": 0,
            "total_doc": 0,
            "saved_file": None,
        }


def process_unit_report(cur_date):
    """
    Fetch data for today and previous day, merge total_doc stats.
    """
    # cur_date = datetime.now().strftime("%Y-%m-%d")
    prev_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    today_result = fetch_unit_report(cur_date)
    prev_result = fetch_unit_report(prev_date)

    combined_total_doc = today_result["total_doc"] + prev_result["total_doc"]

    status = "success" if combined_total_doc > 0 else "no_data"

    return {
        "status": status,
        "total_rows": today_result["total_rows"] or prev_result["total_rows"] or 0,
        "total_columns": today_result["total_columns"] or prev_result["total_columns"] or 0,
        "total_doc": combined_total_doc,
        "saved_file_today": today_result["saved_file"],
        "saved_file_previous": prev_result["saved_file"],
    }


if __name__ == "__main__":
    
    if len(sys.argv) > 1:
        date_arg = sys.argv[1]
    else:
        date_arg = datetime.today().strftime("%Y-%m-%d")

    ans = process_unit_report(date_arg) 
    print(ans)