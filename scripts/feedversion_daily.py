import os
import json
from datetime import datetime
from pyhive import hive
from logging_config1 import start_logging

logger = start_logging("feedFeedVersion_daily_job.log")

def process_feed_feedversion_daily(execution_date: str = None):
    """
    Fetch feed version daily report from Hive and save as JSON.
    Applies no-data skip overwrite rules.
    Returns standardized dict format.
    """
    try:
        # Execution date (Airflow) or today
        date = execution_date or datetime.now().strftime("%Y-%m-%d")
        logger.info(f"FeedFeedVersion Daily ETL started for {date}")

        # Folder & file path
        folder_path = f"/home/hadoop/dhanesh/au_360/au_analytics/feeds/feedFeedVersion_daily/{date}"
        os.makedirs(folder_path, exist_ok=True)
        file_path = os.path.join(folder_path, f"response_{date}.json")

        # Query
        query = f"""
            SELECT * 
            FROM au_360.auw_feedId_version
            WHERE feed_version_date = '{date}'
        """
        logger.info(f"Executing query: {query}")

        # Hive connection
        conn = hive.Connection(
            host="10.59.***", port=10000, username="username", database="au_360"
        )
        cursor = conn.cursor()
        cursor.execute(query)
        api_data = cursor.fetchall()
        column_names = [
            "feedid",
            "feedVersionId",
            "impressions",
            "clicks",
            "unique_users",
            "unique_sessions",
            "ctr",
            "feed_version_date"
        ]

        # Calculate stats
        total_rows = len(api_data)
        total_columns = len(column_names) if total_rows > 0 else 0
        total_doc = total_rows * total_columns if total_rows > 0 else 0

        if total_rows == 0:
            if os.path.exists(file_path):
                logger.warning(f"No new data for {date}. Existing file present → skipping overwrite.")
                status = "no_data_skipped"
            else:
                logger.warning(f"No data found for {date}. Creating empty file.")
                with open(file_path, "w") as jf:
                    json.dump([], jf, indent=4)
                status = "no_data"
        else:
            # Format and save
            data_to_write = [dict(zip(column_names, row)) for row in api_data]
            with open(file_path, "w") as jf:
                json.dump(data_to_write, jf, indent=4)
            logger.info(f"Data saved successfully to {file_path}")
            status = "success"

        cursor.close()
        conn.close()

        logger.info(f"FeedFeedVersion Daily ETL finished for {date}")

        return {
            "status": status,
            "total_rows": total_rows,
            "total_columns": total_columns,
            "total_doc": total_doc,
            "saved_file": file_path if os.path.exists(file_path) else None
        }

    except Exception as e:
        logger.error(f"FeedFeedVersion Daily ETL failed: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "total_rows": 0,
            "total_columns": 0,
            "total_doc": 0,
            "saved_file": None
        }

if __name__ == "__main__":
    
    if len(sys.argv) > 1:
        date_arg = sys.argv[1]
    else:
        date_arg = datetime.today().strftime("%Y-%m-%d")

    ans = process_feed_feedversion_daily(date_arg) 
    print(ans)