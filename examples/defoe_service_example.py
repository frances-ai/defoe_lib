from s3_defoe_service import submit_job

model_name = 'hto'
endpoint = 'http://localhost:3030/hto'


def frequency_distribution():
    query_name = 'frequency_distribution'
    query_config = {'collection': 'Encyclopaedia Britannica', 'level': "edition", 'source': 'NLS'}
    result_file_path = "hto_eb_edition_nls_freqDist.yml"
    job_id = 'hto_eb_nls_freq_dist'
    submit_job(job_id, model_name, query_name, endpoint, query_config, result_file_path)

def frequency_keysearch():
    query_name = 'frequency_keysearch_by_year'
    query_config = {
        'collection': 'Encyclopaedia Britannica',
        'level': "edition",
        'source': 'NLS',
        'data': 'lexicons/animal.txt',
        'end_year': '1773'
    }
    result_file_path = "hto_eb_edition_nls_freqkeywords.yml"
    job_id = 'hto_eb_nls_freq_keysearch'
    submit_job(job_id, model_name, query_name, endpoint, query_config, result_file_path)

if __name__ == "__main__":
    frequency_keysearch()
