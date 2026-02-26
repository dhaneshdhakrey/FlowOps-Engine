import os
import json
from pyhive import hive
from datetime import datetime
from logging_config1 import start_logging

logger = start_logging("city_level_story_report.log")


def process_city_level_story_report(report_date: str):
    """
    Fetch and save City Level Story Report for a given date.
    Returns standardized dict with status, rows, cols, docs.
    """
    try:
        logger.info(f"ETL started for city_level_story_report, report_date={report_date}")

        folder_path = f"/home/hadoop/dhanesh/au_360/au_analytics/city_level_story_report/{report_date}"
        os.makedirs(folder_path, exist_ok=True)

        file_path = os.path.join(folder_path, f"response_{report_date}.json")

        query = f"""
            SELECT * 
            FROM au_360.auw_city_level_story_report
            WHERE report_date = '{report_date}'
        """

        conn = hive.Connection(
            host="10.59.***", port=10000, username="username", database="au_360"
        )
        cursor = conn.cursor()
        cursor.execute(query)

        api_data = cursor.fetchall()
        column_names = [
            "country",
            "state",
            "city",
            "slug",
            "users",
            "views",
            "readtime",
            "report_date",
        ]

        data_to_write = [dict(zip(column_names, row)) for row in api_data]

        total_rows = len(data_to_write)
        total_columns = len(column_names) if total_rows > 0 else 0
        total_doc = total_rows * total_columns if total_rows > 0 else 0

        if total_rows == 0:
            if os.path.exists(file_path):
                logger.warning(
                    f"No new data for city_level_story_report on {report_date}. Existing file found → skipping overwrite."
                )
                status = "no_data_skipped"
            else:
                logger.warning(f"No data found for {report_date}. Creating empty file.")
                with open(file_path, "w") as json_file:
                    json.dump([], json_file, indent=4)
                status = "no_data"
        else:
            with open(file_path, "w") as json_file:
                json.dump(data_to_write, json_file, indent=4)
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
        logger.error(f"ETL failed for {report_date}: {str(e)}", exc_info=True)
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

    ans = process_city_level_story_report(date_arg) 
    print(ans)
