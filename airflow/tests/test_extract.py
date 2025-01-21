import os
import json
import pandas as pd
from tasks.extract import extract_jobs


def test_extract_jobs(tmp_path):
    # Create a temporary source CSV file
    source_file = tmp_path / "jobs.csv"
    data = {
        "context": [
            json.dumps({"title": "Job 1", "description": "Description 1"}),
            json.dumps({"title": "Job 2", "description": "Description 2"}),
        ]
    }
    pd.DataFrame(data).to_csv(source_file, index=False)

    # Run the extract function
    extracted_path = tmp_path / "extracted"
    extract_jobs(source_file)

    # Verify extracted files
    files = list(extracted_path.iterdir())
    assert len(files) == 2
    for idx, file in enumerate(files):
        with open(file, "r") as f:
            extracted_data = json.load(f)
            assert extracted_data["title"] == f"Job {idx + 1}"
            assert extracted_data["description"] == f"Description {idx + 1}"
