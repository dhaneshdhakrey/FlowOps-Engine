import os
import json
from datetime import datetime
from pyhive import hive
from logging_config1 import start_logging

logger=start_logging("stories_views_job.log") 

def process_stories_views(run_date: str):
    """
    Fetch data from Hive for stories_views and save to JSON.
    Returns standardized ETL result dict.
    """
    
    logger.info(f"Starting stories_views ETL for {run_date}", extra={"source": "stories_views_job"})

    folder_path = f"/home/hadoop/dhanesh/au_360/au_analytics/stories_views/{run_date}"
    os.makedirs(folder_path, exist_ok=True)
    file_path = os.path.join(folder_path, f"response_{run_date}.json")

    query = f"""
        SELECT story_id, full_slug, title, concatenated_description, views
        FROM au_360.auw_stories_views
        WHERE report_date='{run_date}'
          AND story_id IS NOT NULL
          AND title IS NOT NULL
          AND concatenated_description IS NOT NULL
    """

    try:
        conn = hive.Connection(
            host='10.59.***',
            port=10000,
            username='hadoop',
            database='au_360'
        )
        cursor = conn.cursor()
        cursor.execute(query)
        api_data = cursor.fetchall()
        cursor.close()
        conn.close()

        columns = ['story_id', 'full_slug', 'title', 'concatenated_description', 'views']
        print(f"----------------------------{api_data}")
        if not api_data:
            # empty case → don’t overwrite file
            if not os.path.exists(file_path):
                with open(file_path, 'w') as json_file:
                    json.dump([], json_file, indent=4)

            logger.warning(f"No data for {run_date}, skipping write", extra={"source": "stories_views_job"})
            return {
                "status": "no_data",
                "total_rows": 0,
                "total_columns": 0,
                "total_doc": 0,
                "saved_file": file_path
            }

        # non-empty
        data_to_write = [dict(zip(columns, row)) for row in api_data]
        with open(file_path, 'w') as json_file:
            json.dump(data_to_write, json_file, indent=4)

        total_rows = len(api_data)
        total_columns = len(columns)
        total_doc = total_rows * total_columns

        logger.info(f"Data saved to {file_path} ({total_rows} rows, {total_columns} cols)", extra={"source": "stories_views_job"})
        return {
            "status": "success",
            "total_rows": total_rows,
            "total_columns": total_columns,
            "total_doc": total_doc,
            "saved_file": file_path
        }

    except Exception as e:
        logger.error(f"ETL failed for {run_date}: {str(e)}", extra={"source": "stories_views_job"})
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

    ans = process_stories_views(date_arg) 
    print(ans)