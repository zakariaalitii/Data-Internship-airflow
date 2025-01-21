import os
import json
from tasks.transform import transform_jobs


def test_transform_jobs(tmp_path):
    # Create temporary extracted files
    extracted_path = tmp_path / "extracted"
    extracted_path.mkdir()
    extracted_data = [
        {"title": "Job 1", "industry": "Tech", "description": "Description 1"},
        {"title": "Job 2", "industry": "Finance", "description": "Description 2"},
    ]
    for idx, data in enumerate(extracted_data):
        with open(extracted_path / f"job_{idx}.txt", "w") as f:
            json.dump(data, f)

    # Run the transform function
    transformed_path = transform_jobs(extracted_path)

    # Verify transformed files
    files = list(transformed_path.iterdir())
    assert len(files) == 2
    for idx, file in enumerate(files):
        with open(file, "r") as f:
            transformed_data = json.load(f)
            assert transformed_data["job"]["title"] == f"Job {idx + 1}"
            assert transformed_data["job"]["industry"] in ["Tech", "Finance"]
