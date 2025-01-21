import os
import json
from airflow.providers.sqlite.hooks.sqlite import SqliteHook


def load_jobs(transformed_path):
    """
    Load the transformed data into the SQLite database.
    """
    sqlite_hook = SqliteHook(sqlite_conn_id="sqlite_default")

    for filename in os.listdir(transformed_path):
        file_path = os.path.join(transformed_path, filename)
        try:
            with open(file_path, "r") as f:
                data = json.load(f)

            sqlite_hook.run(
                """
                INSERT INTO job (title, industry, description, employment_type, date_posted)
                VALUES (:title, :industry, :description, :employment_type, :date_posted)
                """,
                parameters=data["job"],
            )
            job_id = sqlite_hook.get_last_inserted_id()

            sqlite_hook.run(
                """
                INSERT INTO company (job_id, name, link)
                VALUES (:job_id, :name, :link)
                """,
                parameters={"job_id": job_id, **data["company"]},
            )

            if data.get("education"):
                sqlite_hook.run(
                    """
                    INSERT INTO education (job_id, required_credential)
                    VALUES (:job_id, :required_credential)
                    """,
                    parameters={"job_id": job_id, **data["education"]},
                )

            if data.get("experience"):
                sqlite_hook.run(
                    """
                    INSERT INTO experience (job_id, months_of_experience, seniority_level)
                    VALUES (:job_id, :months_of_experience, :seniority_level)
                    """,
                    parameters={"job_id": job_id, **data["experience"]},
                )

            if data.get("salary"):
                sqlite_hook.run(
                    """
                    INSERT INTO salary (job_id, currency, min_value, max_value, unit)
                    VALUES (:job_id, :currency, :min_value, :max_value, :unit)
                    """,
                    parameters={"job_id": job_id, **data["salary"]},
                )

            if data.get("location"):
                sqlite_hook.run(
                    """
                    INSERT INTO location (job_id, country, locality, region, postal_code, street_address, latitude, longitude)
                    VALUES (:job_id, :country, :locality, :region, :postal_code, :street_address, :latitude, :longitude)
                    """,
                    parameters={"job_id": job_id, **data["location"]},
                )

            print(f"Loaded data from {filename}")

        except json.JSONDecodeError:
            print(f"Skipping invalid JSON file: {file_path}")
        except KeyError as e:
            print(f"Missing required key {e} in file: {file_path}")
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
