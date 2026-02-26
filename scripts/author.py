import os
import json
import pandas as pd
from pyhive import hive
from logging_config1 import start_logging

logger = start_logging("author_job.log")

def process_author(execution_date: str):
    """
    execution_date: passed by Airflow in 'YYYY-MM-DD' format
    Returns: dict with status, counts, file info
    """
    date = execution_date
    logger.info(f"Author process started for date {date}")

    # ----------------- Hive Connection -----------------
    try:
        conn = hive.Connection(
            host='10.59.***',
            port=10000,
            username='username',
            database='au_360'
        )
        cursor = conn.cursor()
        logger.info("Hive connection established")
    except Exception as e:
        logger.error(f"Failed to connect to Hive: {str(e)}")
        return {
            "status": "error",
            "error": str(e),
            "total_rows": 0,
            "total_columns": 0,
            "saved_file": None
        }

    # ----------------- Queries -----------------
    queries = {
        "readtime": f"select firstname, lastname, readtime from au_360.auw_gen_author_readtime where report_date='{date}'",
        "stories":  f"select firstname, lastname, stories from au_360.auw_gen_author_stories where report_date='{date}'",
        "users":    f"select firstname, lastname, users from au_360.auw_gen_author_users where report_date='{date}'",
        "views":    f"select firstname, lastname, views from au_360.auw_gen_author_views where report_date='{date}'",
    }

    results = {}
    for name, query in queries.items():
        logger.info(f"Executing query for {name} on {date}")
        cursor.execute(query)
        rows = cursor.fetchall()
        logger.info(f"Fetched {len(rows)} rows for {name}")
        results[name] = rows

    # ----------------- Handle No Data Case -----------------
    dir_name = f"/home/hadoop/dhanesh/au_360/au_analytics/author/{date}"
    os.makedirs(dir_name, exist_ok=True)
    file_path = os.path.join(dir_name, f"response_{date}.json")

    if all(len(v) == 0 for v in results.values()):
        if not os.path.exists(file_path):
            logger.warning(f"No data for {date}, creating empty response file at {file_path}")
            with open(file_path, 'w') as json_file:
                json.dump([], json_file, indent=4)
            return {
                "status": "no_data",
                "total_rows": 0,
                "total_columns": 0,
                "saved_file": file_path
            }
        else:
            logger.warning(f"No data for {date}, response file already exists → skipping overwrite")
            return {
                "status": "no_data_skipped",
                "total_rows": 0,
                "total_columns": 0,
                "total_doc":0,
                "saved_file": file_path
            }

    # ----------------- Merge DataFrames -----------------
    logger.info("Merging DataFrames")

    df_readtime = pd.DataFrame(results["readtime"], columns=['firstname', 'lastname', 'readtime'])
    df_stories  = pd.DataFrame(results["stories"],  columns=['firstname', 'lastname', 'stories'])
    df_users    = pd.DataFrame(results["users"],    columns=['firstname', 'lastname', 'users'])
    df_views    = pd.DataFrame(results["views"],    columns=['firstname', 'lastname', 'views'])

    df_combined = df_stories.merge(df_readtime, on=['firstname', 'lastname'], how='left')
    df_combined = df_combined.merge(df_users, on=['firstname', 'lastname'], how='left')
    df_combined = df_combined.merge(df_views, on=['firstname', 'lastname'], how='left')

    # Clean
    df_combined = df_combined.dropna(subset=['firstname', 'lastname'])
    df_combined['full_name'] = df_combined['firstname'] + ' ' + df_combined['lastname']
    df_combined = df_combined.drop(columns=['firstname', 'lastname'])

    total_rows = len(df_combined)
    total_columns = len(df_combined.columns)
    logger.info(f"Final combined rows: {total_rows}, columns: {total_columns}")

    # ----------------- Save JSON -----------------
    result_dict = df_combined.to_dict(orient='records')
    with open(file_path, 'w') as json_file:
        json.dump(result_dict, json_file, indent=4)

    logger.info(f"Data saved successfully to {file_path}")
    total_doc=total_rows*total_columns
    return {
        "status": "success",
        "total_rows": total_rows,
        "total_columns": total_columns,
        "total_doc":total_doc,
        "saved_file": file_path
    }
    
if __name__ == "__main__":
    
    if len(sys.argv) > 1:
        date_arg = sys.argv[1]
    else:
        date_arg = datetime.today().strftime("%Y-%m-%d")

    ans = process_author(date_arg) 
    print(ans)
