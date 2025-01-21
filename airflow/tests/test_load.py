import json
from unittest.mock import MagicMock, patch
from airflow.dags.tasks.load import load_jobs


@patch("tasks.load.SqliteHook")
def test_load_jobs(mock_sqlite_hook, tmp_path):
    transformed_path = tmp_path / "transformed"
    transformed_path.mkdir()
    transformed_data = [
        {
            "job": {
                "title": "Job 1",
                "industry": "Tech",
                "description": "Description 1",
            },
            "company": {"name": "Company 1", "link": "http://company1.com"},
        },
        {
            "job": {
                "title": "Job 2",
                "industry": "Finance",
                "description": "Description 2",
            },
            "company": {"name": "Company 2", "link": "http://company2.com"},
        },
    ]
    for idx, data in enumerate(transformed_data):
        with open(transformed_path / f"job_{idx}.json", "w") as f:
            json.dump(data, f)

    mock_hook_instance = MagicMock()
    mock_sqlite_hook.return_value = mock_hook_instance

    load_jobs(str(transformed_path))

    assert mock_hook_instance.run.call_count == 4  
    mock_hook_instance.run.assert_any_call(
        """
        INSERT INTO job (title, industry, description, employment_type, date_posted)
        VALUES (:title, :industry, :description, :employment_type, :date_posted)
        """,
        parameters={
            "title": "Job 1",
            "industry": "Tech",
            "description": "Description 1",
        },
    )
    mock_hook_instance.run.assert_any_call(
        """
        INSERT INTO company (job_id, name, link)
        VALUES (:job_id, :name, :link)
        """,
        parameters={"job_id": 1, "name": "Company 1", "link": "http://company1.com"},
    )
