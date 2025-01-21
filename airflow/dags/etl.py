from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from tasks.extract import extract_jobs
from tasks.transform import transform_jobs
from tasks.load import load_jobs
import os

SOURCE_FILE = "/opt/airflow/source/jobs.csv"
EXTRACTED_PATH = "/opt/airflow/staging/extracted"
TRANSFORMED_PATH = "/opt/airflow/staging/transformed"

TABLES_CREATION_QUERIES = [
    """
    CREATE TABLE IF NOT EXISTS job (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title VARCHAR(225),
        industry VARCHAR(225),
        description TEXT,
        employment_type VARCHAR(125),
        date_posted DATE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS company (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        name VARCHAR(225),
        link TEXT,
        FOREIGN KEY (job_id) REFERENCES job(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS education (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        required_credential VARCHAR(225),
        FOREIGN KEY (job_id) REFERENCES job(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS experience (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        months_of_experience INTEGER,
        seniority_level VARCHAR(25),
        FOREIGN KEY (job_id) REFERENCES job(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS salary (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        currency VARCHAR(3),
        min_value NUMERIC,
        max_value NUMERIC,
        unit VARCHAR(12),
        FOREIGN KEY (job_id) REFERENCES job(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS location (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        country VARCHAR(60),
        locality VARCHAR(60),
        region VARCHAR(60),
        postal_code VARCHAR(25),
        street_address VARCHAR(225),
        latitude NUMERIC,
        longitude NUMERIC,
        FOREIGN KEY (job_id) REFERENCES job(id)
    );
    """,
]

DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15),
}

@task()
def extract():
    """Extract job data from the source file."""
    if not os.path.exists(SOURCE_FILE):
        raise FileNotFoundError(f"Source file not found: {SOURCE_FILE}")
    extracted_path = extract_jobs(SOURCE_FILE)
    if not os.path.isdir(extracted_path):
        raise ValueError(
            f"Extracted path does not exist or is not a directory: {extracted_path}"
        )
    print(f"Extracted files to: {extracted_path}")
    return extracted_path

@task()
def transform(extracted_path):
    """Transform the extracted data."""
    if not os.path.isdir(extracted_path):
        raise ValueError(f"Invalid extracted path: {extracted_path}")
    transformed_path = transform_jobs(extracted_path)
    if not os.path.isdir(transformed_path):
        raise ValueError(
            f"Transformed path does not exist or is not a directory: {transformed_path}"
        )
    print(f"Transformed files to: {transformed_path}")
    return transformed_path

@task()
def load(transformed_path):
    """Load the transformed data into SQLite."""
    if not os.path.isdir(transformed_path):
        raise ValueError(f"Invalid transformed path: {transformed_path}")
    load_jobs(transformed_path)
    print(f"Data loaded from transformed path: {transformed_path}")

@dag(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    schedule="@daily",
    start_date=datetime(2025, 1, 21),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS,
)
def etl_dag():
    """Define the ETL pipeline."""

    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERIES, 
    )

    extracted_path = extract()
    transformed_path = transform(extracted_path)
    create_tables >> extracted_path >> transformed_path >> load(transformed_path)

etl_pipeline = etl_dag()
