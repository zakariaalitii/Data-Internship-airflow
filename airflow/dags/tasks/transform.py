import os
import json
import re

TRANSFORMED_PATH = "/opt/airflow/staging/transformed"

def clean_description(description):
    """Clean the job description by removing unwanted characters and whitespace."""
    if not description:
        return ""
    return re.sub(r"\s+", " ", description).strip()

def transform_jobs(extracted_path):
    """
    Read the extracted text files as JSON, clean the data, transform the schema, 
    and save each item to the staging/transformed directory as JSON files.
    """
    os.makedirs(TRANSFORMED_PATH, exist_ok=True)

    for filename in os.listdir(extracted_path):
        file_path = f"{extracted_path}/{filename}"
        
        try:
            with open(file_path, "r") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Invalid JSON in file: {file_path}. Error: {e}")
            continue  

        experience_requirements = data.get("experienceRequirements", {})
        if isinstance(experience_requirements, str):
            experience_requirements = {}  

      
        transformed_data = {
            "job": {
                "title": data.get("title"),
                "industry": data.get("industry"),
                "description": clean_description(data.get("description")),
                "employment_type": data.get("employmentType"),
                "date_posted": data.get("datePosted"),
            },
            "company": {
                "name": data.get("hiringOrganization", {}).get("name"),
                "link": data.get("hiringOrganization", {}).get("sameAs"),
            },
            "education": {
                "required_credential": data.get("educationRequirements", {}).get(
                    "credentialCategory"
                ),
            },
            "experience": {
                "months_of_experience": experience_requirements.get("monthsOfExperience"),
                "seniority_level": data.get("seniorityLevel"),
            },
            "salary": {
                "currency": data.get("baseSalary", {}).get("currency"),
                "min_value": data.get("baseSalary", {})
                .get("value", {})
                .get("minValue"),
                "max_value": data.get("baseSalary", {})
                .get("value", {})
                .get("maxValue"),
                "unit": data.get("baseSalary", {}).get("value", {}).get("unitText"),
            },
            "location": {
                "country": data.get("jobLocation", {})
                .get("address", {})
                .get("addressCountry"),
                "locality": data.get("jobLocation", {})
                .get("address", {})
                .get("addressLocality"),
                "region": data.get("jobLocation", {})
                .get("address", {})
                .get("addressRegion"),
                "postal_code": data.get("jobLocation", {})
                .get("address", {})
                .get("postalCode"),
                "street_address": data.get("jobLocation", {})
                .get("address", {})
                .get("streetAddress"),
                "latitude": data.get("jobLocation", {}).get("latitude"),
                "longitude": data.get("jobLocation", {}).get("longitude"),
            },
        }

        output_file = f"{TRANSFORMED_PATH}/{filename.replace('.txt', '.json')}"
        with open(output_file, "w") as f:
            json.dump(transformed_data, f, indent=4)

        print(f"Transformed and saved: {output_file}")

    return TRANSFORMED_PATH
