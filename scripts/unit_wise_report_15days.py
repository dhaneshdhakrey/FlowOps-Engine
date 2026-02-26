import os
import json
from datetime import datetime
from pyhive import hive
from logging_config1 import start_logging

logger = start_logging("author_job.log")


def process_unit_wise_report_15days(cur_date):
    # cur_date = datetime.now().strftime("%Y-%m-%d")
    folder_path = f"/home/hadoop/dhanesh/au_360/au_analytics/unit_wise_report_15days/{cur_date}"
    os.makedirs(folder_path, exist_ok=True)
    logger.info(f"Processing unit_wise_report_15days for date: {cur_date}")
    file_path = os.path.join(folder_path, f"response_{cur_date}.json")
    query = f"SELECT * FROM au_360.unit_vise_report_15days WHERE report_date='{cur_date}'"

    conn = hive.Connection(
        host="10.59.***",
        port=10000,
        username="hadoop",
        database="au_360"
    )
    cursor = conn.cursor()

    try:
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
            "report_date"
        ]
        logger.info(f"Fetched {len(api_data)} rows for date: {cur_date}")
        if not api_data:
            if os.path.exists(file_path):
                # Skip writing if empty and file already exists
                return {
                    "status": "success",
                    "message": "No new data, existing file kept.",
                    "total_rows": 0,
                    "total_columns": len(column_names),
                    "total_doc": 0,
                    "saved_file": file_path
                }
            else:
                # Save empty file if nothing exists
                with open(file_path, "w") as json_file:
                    json.dump([], json_file, indent=4)
                return {
                    "status": "success",
                    "message": "No data, saved empty JSON file.",
                    "total_rows": 0,
                    "total_columns": len(column_names),
                    "total_doc": 0,
                    "saved_file": file_path
                }

        data_to_write = [dict(zip(column_names, row)) for row in api_data]

        with open(file_path, "w") as json_file:
            json.dump(data_to_write, json_file, indent=4)

        total_rows = len(api_data)
        total_columns = len(column_names)
        total_doc = total_rows * total_columns

        return {
            "status": "success",
            "total_rows": total_rows,
            "total_columns": total_columns,
            "total_doc": total_doc,
            "saved_file": file_path
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "total_rows": 0,
            "total_columns": 0,
            "total_doc": 0,
            "saved_file": None
        }

    finally:
        cursor.close()
        conn.close()



if __name__ == "__main__":
    
    if len(sys.argv) > 1:
        date_arg = sys.argv[1]
    else:
        date_arg = datetime.today().strftime("%Y-%m-%d")

    ans = process_unit_wise_report_15days(date_arg) 
    print(ans)