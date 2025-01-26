from google.cloud import dataproc_v1 as dataproc


def submit_job(project_id, region, cluster_name):
    # Create the job client.
    job_client = dataproc.JobControllerClient(
        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
    )

    # Create the job config. 'main_jar_file_uri' can also be a
    # Google Cloud Storage URL.
    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {
            "main_python_file_uri": "gs://frances2023/run_query.py",
            "python_file_uris": ["gs://frances2023/defoe.zip"],
            "args": ["--query_name=frequency_keysearch_by_year",
                     "--model_name=sparql",
                     "--endpoint=http://34.105.180.58:3030/total_eb/sparql",
                     "--kg_type=total_eb",
                     "--start_year=1780",
                     "--end_year=1800",
                     "--data=animal.txt",
                     "--result_file_path=animal_result.yml"
                     ]
        },
    }

    operation = job_client.submit_job_as_operation(
        request={"project_id": project_id, "region": region, "job": job}
    )
    response = operation.result()
    print("Job finished!")
    print(response)


if __name__ == "__main__":
    PROJECT_ID = "frances-365422"
    REGION = "europe-central2"
    CLUSTER_NAME = "cluster-c001"
    # authenticate_implicit_with_adc(PROJECT_ID)
    submit_job(PROJECT_ID, REGION, CLUSTER_NAME)
