import os
import pandas as pd
import json

EXTRACTED_PATH = "/opt/airflow/staging/extracted"


def extract_jobs(source_file):
    print(f"Source file: {source_file}")
    os.makedirs(EXTRACTED_PATH, exist_ok=True)
    print(f"Extracted path created: {EXTRACTED_PATH}")
    df = pd.read_csv(source_file)

    for index, row in df.iterrows():
        context = row.get("context")
        if pd.isna(context):  
            print(f"Skipping row {index} due to missing context.")
            continue
        try:
            parsed_context = json.loads(context) 
        except (TypeError, json.JSONDecodeError) as e:
            print(f"Skipping row {index} due to invalid JSON: {e}")
            continue

        with open(f"{EXTRACTED_PATH}/job_{index}.txt", "w") as f:
            f.write(json.dumps(parsed_context))

    return EXTRACTED_PATH
