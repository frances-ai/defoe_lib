from defoe_lib.config import DefoeConfig
from defoe_lib.service import DefoeService

vars = {
    "sparkUrl": "local[*]",
    "fusekiUrl": "http://localhost:3030/total_eb/sparql",
    "chapbooksScotlandUrl": "http://localhost:3030/chapbooks_scotland/sparql"
}

defoe_config = DefoeConfig.from_dict(vars)
service = DefoeService(defoe_config)

print(service.config.spark_url)


def get_config():
    config = {
        "preprocess": "none",
        "start_year": "1771",
        "end_year": "1781",
        "hit_count": "term",
        "data": "/Users/ly40/Documents/frances-ai/defoe_lib/queries/animal.txt",
        "kg_type": "total_eb",
        "result_file_path": "/Users/ly40/Documents/frances-ai/defoe_lib/result_animal.yml"
    }
    return config


def submit():
    return service.submit_job("12", "sparql", "frequency_keysearch_by_year", get_config())
