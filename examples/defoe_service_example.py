from defoe_service import submit_job

if __name__ == "__main__":
    model_name = 'hto'
    endpoint = 'http://query.frances-ai.com/hto'
    query_name = 'frequency_distribution'
    query_config = {'collection': 'Encyclopaedia Britannica', 'level': "edition", 'source': 'NLS'}
    result_file_path = "hto_eb_edition_nls_freqDist.yml"
    job_id = 'hto_eb_nls_freq_dist'
    submit_job(job_id, model_name, query_name, endpoint, query_config, result_file_path)