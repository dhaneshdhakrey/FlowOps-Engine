import os
import json
from datetime import datetime
from pyhive import hive
from logging_config1 import start_logging  # 


logger = start_logging("feed_hourly_job.log")
def process_feed_hourly(run_date: str):
    """
    Fetch feed_hourly data from Hive and save as JSON.
    Returns standardized ETL result dict.
    """
    
    logger.info(f"Starting feed_hourly ETL for {run_date}", extra={"source": "feed_hourly_job"})

    folder_path = f"/home/hadoop/dhanesh/au_360/au_analytics/feeds/feed_hourly/{run_date}"
    os.makedirs(folder_path, exist_ok=True)
    file_path = os.path.join(folder_path, f"response_{run_date}.json")

    query = f"""
        SELECT time_window, feedid, impressions, clicks, unique_users,
               unique_sessions, ctr, feed_hourly_date
        FROM au_360.auw_feed_hourly_report
        WHERE feed_hourly_date='{run_date}'
    """

    try:
        conn = hive.Connection(
            host='10.59.***',
            port=10000,
            username='username',
            database='au_360'
        )
        cursor = conn.cursor()
        cursor.execute(query)
        api_data = cursor.fetchall()
        cursor.close()
        conn.close()

        column_names = [
            "time_window",
            "feedid",
            "impressions",
            "clicks",
            "unique_users",
            "unique_sessions",
            "ctr",
            "feed_hourly_date"
        ]

        if not api_data:
            # empty case → don’t overwrite existing non-empty file
            if not os.path.exists(file_path):
                with open(file_path, 'w') as json_file:
                    json.dump([], json_file, indent=4)

            logger.warning(f"No feed_hourly data for {run_date}, skipping write",
                           extra={"source": "feed_hourly_job"})
            return {
                "status": "no_data_skipped",
                "total_rows": 0,
                "total_columns": 0,
                "total_doc": 0,
                "saved_file": file_path
            }

        # non-empty case
        data_to_write = [dict(zip(column_names, row)) for row in api_data]
        with open(file_path, 'w') as json_file:
            json.dump(data_to_write, json_file, indent=4)

        total_rows = len(api_data)
        total_columns = len(column_names)
        total_doc = total_rows * total_columns

        logger.info(f"Saved {total_rows} rows to {file_path}",
                    extra={"source": "feed_hourly_job"})
        return {
            "status": "success",
            "total_rows": total_rows,
            "total_columns": total_columns,
            "total_doc": total_doc,
            "saved_file": file_path
        }

    except Exception as e:
        logger.error(f"ETL failed for {run_date}: {str(e)}", extra={"source": "feed_hourly_job"})
        return {
            "status": "error",
            "total_rows": 0,
            "total_columns": 0,
            "total_doc": 0,
            "saved_file": None,
            "error": str(e)
        }
        
if __name__ == "__main__":
    
    if len(sys.argv) > 1:
        date_arg = sys.argv[1]
    else:
        date_arg = datetime.today().strftime("%Y-%m-%d")

    ans = process_feed_hourly(date_arg) 
    print(ans)
