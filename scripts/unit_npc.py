
import json
from pyhive import hive
from datetime import datetime
import os
import sys

from logging_config1 import start_logging

logger = start_logging("unit_npc.log")

def run_unit_npc(execution_date: str):
    """
    execution_date: date string in 'YYYY-MM-DD' format
    Returns: dict with status, file info, row count
    """
    logger.info(f"Unit NPC job started for date {execution_date}")
    try:
        # unit city npc
        folder_city = f"/home/hadoop/dhanesh/au_360/au_analytics/unit_city_npc/{execution_date}"
        os.makedirs(folder_city, exist_ok=True)
        file_city = os.path.join(folder_city, f"response_{execution_date}.json")
        query_city = f"select story_unit, story_city, newspro_category_title, story_count, video_count, no_videos, no_media, users, views, duration_value from unit_generic_report where grain_level = 'unit_city_newsproct' and duration_type='daily' and duration_value = '{execution_date}'"

        # unit npc
        folder_npc = f"/home/hadoop/dhanesh/au_360/au_analytics/unit_npc/{execution_date}"
        os.makedirs(folder_npc, exist_ok=True)
        file_npc = os.path.join(folder_npc, f"response_{execution_date}.json")
        query_npc = f"select story_unit, newspro_category_title, story_count, video_count, no_videos, no_media, users, views, duration_value from unit_generic_report where grain_level = 'unit_newsproct' and duration_type='daily' and duration_value = '{execution_date}'"

        try:
            conn = hive.Connection(host='10.59.***', port=10000, username='username', database='au_360')
            cursor = conn.cursor()
        except Exception as conn_err:
            logger.error(f"Failed to connect to Hive: {str(conn_err)}")
            return {
                "status": "error",
                "total_rows": 0,
                "total_columns": 0,
                "total_doc": 0,
                "saved_file": None
            }

        # unit city npc
        cursor.execute(query_city)
        api_data_city = cursor.fetchall()
        columns_city = ['story_unit','story_city','newspro_category_title','story_count','video_count','no_videos','no_media', 'users', 'views','report_date']
        data_city = [dict(zip(columns_city, row)) for row in api_data_city]
        with open(file_city, 'w') as json_file:
            json.dump(data_city, json_file, indent=4)

        # unit npc
        cursor.execute(query_npc)
        api_data_npc = cursor.fetchall()
        columns_npc = ['story_unit','newspro_category_title','story_count','video_count','no_videos','no_media', 'users', 'views','report_date']
        data_npc = [dict(zip(columns_npc, row)) for row in api_data_npc]
        with open(file_npc, 'w') as json_file:
            json.dump(data_npc, json_file, indent=4)

        cursor.close()
        conn.close()

        result = {
            "status": "success",
            "unit_city_npc": {
                "total_rows": len(data_city),
                "total_columns": len(columns_city),
                "total_doc": len(data_city) * len(columns_city),
                "saved_file": file_city
            },
            "unit_npc": {
                "total_rows": len(data_npc),
                "total_columns": len(columns_npc),
                "total_doc": len(data_npc) * len(columns_npc),
                "saved_file": file_npc
            }
        }
        logger.info(f"Unit NPC job completed for date {execution_date} with status: success")
        return result
    except Exception as e:
        logger.error(f"Unit NPC job failed for date {execution_date}: {str(e)}", exc_info=True)
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

    ans = run_unit_npc(date_arg)
    logger.info(f"Unit NPC job result: {ans}")
    print(ans)