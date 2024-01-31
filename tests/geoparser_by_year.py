from config import DefoeConfig
from defoe_service import LocalDefoeService

vars = {
    "sparkUrl": "local[*]",
    "fusekiUrl": "http://35.228.63.82:3030/total_eb/sparql",
    "chapbooksScotlandUrl": "http://35.228.63.82:3030/chapbooks_scotland/sparql"
}

defoe_config = DefoeConfig.from_dict(vars)
service = LocalDefoeService(defoe_config)

print(service.config.spark_url)


def get_config():
    config = {
                 "kg_type": "chapbooks_scotland",
                "gazetteer": "geonames",
                "preprocess": "normalize",
                "data": "/Users/ly40/Documents/frances-ai/defoe_lib/queries/animal.txt",
                "start_year": '1800',
                "end_year": '1800',
                "result_file_path": "/Users/ly40/Documents/frances-ai/defoe_lib/result_animal_geo.yml"
    }
    return config


def submit():
    print("submit")
    return submit_job("12", "sparql", "geoparser_by_year", get_config())
