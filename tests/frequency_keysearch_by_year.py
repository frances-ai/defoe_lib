from config import DefoeConfig
from defoe_service import LocalDefoeService

vars = {
    "sparkUrl": "local[*]",
    "fusekiUrl": "http://localhost:3030/total_eb/sparql",
    "chapbooksScotlandUrl": "http://localhost:3030/chapbooks_scotland/sparql"
}

defoe_config = DefoeConfig.from_dict(vars)
service = LocalDefoeService(defoe_config)

print(service.config.spark_url)


def get_config():
    config = {
        "start_year": "1768",
        "end_year": "1860",
        "hit_count": "term",
        "preprocess": "lemmatize",
        "data": "/Users/ly40/Documents/PhD/lexicon/commodities.txt",
        "kg_type": "total_eb",
        "result_file_path": "/Users/ly40/Documents/frances-ai/defoe_lib/result_commodities.yml"
    }
    return config


def submit():
    return submit_job("12", "sparql", "frequency_keysearch_by_year", get_config())


if __name__ == "__main__":
    submit()
