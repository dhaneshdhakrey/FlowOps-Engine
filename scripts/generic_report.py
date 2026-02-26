import os
import json
import requests
from logging_config1 import start_logging

logger = start_logging("generic_report_job.log")

DIMENSIONS = ["city", "url", "slug"]

def process_generic_report(execution_date: str):
    """
    Fetch and save generic report data for all dimensions (city, url, slug).
    Returns a single standardized object.
    """
    date = execution_date
    logger.info(f"Generic report ETL started for date {date}")

    # parent folder
    folder_path = f"/home/hadoop/dhanesh/au_360/au_analytics/generic_report/{date}"
    os.makedirs(folder_path, exist_ok=True)

    index_file = os.path.join(folder_path, f"summary_{date}.json")
    results = []
    total_rows, total_columns, total_doc = 0, 0, 0
    overall_status = "no_data"

    for dimension in DIMENSIONS:
        api_url = f"http://10.59.***:6601/generic_auw_report/date={date}/dimension={dimension}"
        dim_folder = os.path.join(folder_path, dimension)
        os.makedirs(dim_folder, exist_ok=True)

        file_path = os.path.join(dim_folder, f"response_{date}.json")
        logger.info(f"Fetching {dimension} data from {api_url}")

        try:
            response = requests.get(api_url, timeout=60)

            if response.status_code == 200:
                api_data = response.json()

                if len(api_data) == 0:
                    logger.warning(f"{dimension}: Empty API response")
                    # no rows/cols
                    dim_rows, dim_cols, dim_doc = 0, 0, 0
                    status = "no_data"
                    # still save empty file for tracking
                    with open(file_path, 'w') as json_file:
                        json.dump([], json_file, indent=4)
                else:
                    logger.info(f"{dimension}: Data received, saving → {file_path}")
                    with open(file_path, 'w') as json_file:
                        json.dump(api_data, json_file, indent=4)
                    # dimension-based → treat as 1×1
                    dim_rows, dim_cols, dim_doc = 1, 1, 1
                    status = "success"

                total_rows += dim_rows
                total_columns += dim_cols
                total_doc += dim_doc

                if status == "success":
                    overall_status = "success"

                results.append({
                    "dimension": dimension,
                    "status": status,
                    "records": len(api_data),
                    "saved_file": file_path
                })

            else:
                logger.error(f"{dimension}: Failed with status code {response.status_code}")
                results.append({
                    "dimension": dimension,
                    "status": "error",
                    "records": 0,
                    "saved_file": None
                })
                overall_status = "error"

        except requests.exceptions.RequestException as e:
            logger.error(f"{dimension}: Request error → {str(e)}")
            results.append({
                "dimension": dimension,
                "status": "error",
                "records": 0,
                "saved_file": None
            })
            overall_status = "error"

    # save index file for reference
    with open(index_file, "w") as jf:
        json.dump(results, jf, indent=4)

    logger.info(f"Generic report ETL finished for {date}")

    return {
        "status": overall_status,
        "total_rows": total_rows,
        "total_columns": total_columns,
        "total_doc": total_doc,
        "saved_file": index_file
    }

if __name__ == "__main__":
    
    if len(sys.argv) > 1:
        date_arg = sys.argv[1]
    else:
        date_arg = datetime.today().strftime("%Y-%m-%d")

    ans = process_generic_report(date_arg) 
    print(ans)