import os
import json
import shutil
from datetime import datetime
from pyhive import hive
from logging_config1 import start_logging

logger = start_logging("unit_city_stories_metrics_lastN.log")

OUTPUT_BASE = "/home/hadoop/au_360/dhanesh/au_analytics/unit_city_stories_metrics_lastN"
REQUIRED_KEYS = ["slug", "story_unit", "story_city", "users", "views"]
AGGREGATION_TYPES = ["last7days", "last15days", "last30days", "last60days"]


def save_lastN_metrics():
    """
    ETL job for unit_city_stories_metrics (lastN aggregations).
    Returns standardized dicts for each agg_type.
    """
    report_date = datetime.today().strftime("%Y-%m-%d")
    results = {}

    try:
        conn = hive.Connection(host="10.59.***", port=10000, username="username", database="au_360")
        cursor = conn.cursor()

        for agg_type in AGGREGATION_TYPES:
            try:
                query = f"""
                    SELECT full_slug as slug, story_unit, story_city, users, views
                    FROM au_360.unit_city_stories_metrics
                    WHERE aggregation_type = '{agg_type}'
                    AND report_date = '{report_date}'
                """
                logger.info(f"Running query for aggregation_type={agg_type}, report_date={report_date}")
                cursor.execute(query)

                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()

                if not rows:
                    folder_path = os.path.join(OUTPUT_BASE, f"aggregation_type={agg_type}", f"report_date={report_date}")
                    file_path = os.path.join(folder_path, "data.json")

                    if os.path.exists(file_path):
                        logger.warning(f"No new data for {agg_type} → Keeping old file {file_path}")
                        results[agg_type] = {
                            "status": "no_data_skipped",
                            "total_rows": 0,
                            "total_columns": 0,
                            "total_doc": 0,
                            "saved_file": file_path
                        }
                        continue
                    else:
                        logger.warning(f"No data for {agg_type} → Creating empty file at {file_path}")
                        os.makedirs(folder_path, exist_ok=True)
                        with open(file_path, "w") as f:
                            json.dump([], f)
                        results[agg_type] = {
                            "status": "no_data",
                            "total_rows": 0,
                            "total_columns": 0,
                            "total_doc": 0,
                            "saved_file": file_path
                        }
                        continue

                # Convert to desired dict structure
                data = []
                for row in rows:
                    row_dict = dict(zip(columns, row))
                    record = {
                        key: row_dict.get(key, "" if key in ["slug", "story_unit", "story_city"] else 0)
                        for key in REQUIRED_KEYS
                    }
                    data.append(record)

                total_rows = len(data)
                total_columns = len(REQUIRED_KEYS)
                total_doc = total_rows * total_columns

                # Delete old report_date folders
                agg_base_path = os.path.join(OUTPUT_BASE, f"aggregation_type={agg_type}")
                if os.path.exists(agg_base_path):
                    for entry in os.listdir(agg_base_path):
                        if entry.startswith("report_date="):
                            full_path = os.path.join(agg_base_path, entry)
                            if os.path.isdir(full_path):
                                shutil.rmtree(full_path)
                                logger.info(f"Deleted old folder: {full_path}")

                # Save fresh file
                folder_path = os.path.join(agg_base_path, f"report_date={report_date}")
                os.makedirs(folder_path, exist_ok=True)
                file_path = os.path.join(folder_path, "data.json")

                with open(file_path, "w") as f:
                    for record in data:
                        f.write(json.dumps(record) + "\n")

                logger.info(f"✅ Saved {total_rows} rows for {agg_type} to {file_path}")

                results[agg_type] = {
                    "status": "success",
                    "total_rows": total_rows,
                    "total_columns": total_columns,
                    "total_doc": total_doc,
                    "saved_file": file_path
                }

            except Exception as e:
                logger.error(f"❌ Error processing {agg_type}: {str(e)}", exc_info=True)
                results[agg_type] = {
                    "status": "error",
                    "total_rows": 0,
                    "total_columns": 0,
                    "total_doc": 0,
                    "saved_file": None
                }

    finally:
        cursor.close()
        conn.close()

    # aggregate counts across all agg_types
        total_rows = sum(v.get("total_rows", 0) for v in results.values())
        total_columns = sum(v.get("total_columns", 0) for v in results.values())
        total_doc = sum(v.get("total_doc", 0) for v in results.values())

    return {
        "status": "success" if any(v["status"] == "success" for v in results.values()) else "no_data",
        "total_rows": total_rows,
        "total_columns": total_columns,
        "total_doc": total_doc,
        "saved_file": None  # multiple files possible, so keeping None or change to list if needed
    }

if __name__ == "__main__":
    
    if len(sys.argv) > 1:
        date_arg = sys.argv[1]
    else:
        date_arg = datetime.today().strftime("%Y-%m-%d")

    ans = save_lastN_metrics() 
    print(ans)