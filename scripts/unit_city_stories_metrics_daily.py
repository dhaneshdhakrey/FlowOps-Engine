import os
import json
from pyhive import hive
from datetime import datetime
from logging_config1 import start_logging

logger = start_logging("unit_city_stories_metrics_daily.log")


def process_unit_city_stories_metrics_daily(report_date: str, agg_type: str = "daily"):
    """
    Fetch and save Unit City Stories Metrics for a given date and aggregation type.
    Returns standardized dict with status, rows, cols, docs.
    """
    try:
        logger.info(f"ETL started for aggregation_type={agg_type}, report_date={report_date}")

        output_base = "/home/hadoop/dhanesh/au_360/au_analytics/unit_city_stories_metrics_daily"
        folder_path = os.path.join(output_base, f"aggregation_type={agg_type}", f"report_date={report_date}")
        os.makedirs(folder_path, exist_ok=True)

        output_file = os.path.join(folder_path, "data.json")

        query = f"""
            SELECT full_slug as slug, story_unit, story_city, users, views
            FROM au_360.unit_city_stories_metrics
            WHERE aggregation_type = '{agg_type}'
            AND report_date = '{report_date}'
        """

        conn = hive.Connection(
            host="10.59.***", port=10000, username="username", database="au_360"
        )
        cursor = conn.cursor()
        cursor.execute(query)

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        required_keys = ["slug", "story_unit", "story_city", "users", "views"]

        data = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            # Fill blanks: text fields → "", numeric fields → 0
            record = {
                key: row_dict.get(key, "" if key in ["slug", "story_unit", "story_city"] else 0)
                for key in required_keys
            }
            data.append(record)

        total_rows = len(data)
        total_columns = len(required_keys) if total_rows > 0 else 0
        total_doc = total_rows * total_columns if total_rows > 0 else 0

        if total_rows == 0:
            if os.path.exists(output_file):
                logger.warning(
                    f"No new data for {agg_type} on {report_date}. Existing file found → skipping overwrite."
                )
                status = "no_data_skipped"
            else:
                logger.warning(f"No data found for {agg_type} on {report_date}. Creating empty file.")
                with open(output_file, "w") as f:
                    json.dump([], f, indent=4)
                status = "no_data"
        else:
            # Write as line-delimited JSON (per original script)
            with open(output_file, "w") as f:
                for record in data:
                    f.write(json.dumps(record) + "\n")
            logger.info(f"Data saved successfully to {output_file}")
            status = "success"

        cursor.close()
        conn.close()

        return {
            "status": status,
            "total_rows": total_rows,
            "total_columns": total_columns,
            "total_doc": total_doc,
            "saved_file": output_file if os.path.exists(output_file) else None,
        }

    except Exception as e:
        logger.error(
            f"ETL failed for {agg_type} on {report_date}: {str(e)}",
            exc_info=True,
        )
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

    ans = process_unit_city_stories_metrics_daily(date_arg,agg_type="daily") 
    print(ans)