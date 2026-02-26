import os
import json
from pyhive import hive
from datetime import datetime
from logging_config1 import start_logging

logger = start_logging("unit_wise_report_monthly.log")


def process_unit_wise_report_monthly(report_month: str):
    """
    Fetch and save Unit Wise Report (monthly) for a given month.
    Returns standardized dict format.
    """
    try:
        
        logger.info(f"Unit Wise Report Monthly ETL started for {report_month}")

        folder_path = f"/home/hadoop/au_360/dhanesh/au_analytics/unit_wise_report_monthly/{report_month}"
        os.makedirs(folder_path, exist_ok=True)
        file_path = os.path.join(folder_path, f"response_{report_month}.json")

        query = f"SELECT * FROM au_360.unit_vise_report_monthly WHERE report_month='{report_month}'"

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
            "views",
            "users",
            "report_month",
        ]

        data_to_write = [dict(zip(column_names, row)) for row in api_data]

        total_rows = len(data_to_write)
        total_columns = len(column_names) if total_rows > 0 else 0
        total_doc = total_rows * total_columns if total_rows > 0 else 0

        if total_rows == 0:
            if os.path.exists(file_path):
                logger.warning(
                    f"No new data for {report_month}. Existing file found → skipping overwrite."
                )
                status = "no_data_skipped"
            else:
                logger.warning(f"No data found for {report_month}. Creating empty file.")
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
        logger.error(
            f"Unit Wise Report Monthly ETL failed for {report_month}: {str(e)}",
            exc_info=True,
        )
        return {
            "status": "error",
            "total_rows": 0,
            "total_columns": 0,
            "total_doc": 0,
            "saved_file": None,
        }


# def process_unit_wise_report_monthly():
#     """
#     Process current month’s unit-wise report.
#     """
#     current_month_name = datetime.now().strftime("%B")
#     return fetch_unit_wise_report_monthly(current_month_name)

if __name__ == "__main__":
    
    if len(sys.argv) > 1:
        date_arg = sys.argv[1]
    else:
        date_arg = datetime.today().strftime("%Y-%m-%d")
    
    current_month_name = datetime.now().strftime("%B")

    ans = process_unit_wise_report_monthly(current_month_name) 
    print(ans)