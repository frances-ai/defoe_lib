from defoe_lib.config import DefoeConfig
from defoe_lib.service import DefoeService

empty_yaml = "--- !!str"
fuseki_url = "http://localhost:3030/total_eb/sparql"


c = DefoeConfig("local[1]")
s = DefoeService(c)
j = s.submit_job(
  job_id="wpa123",
  model_name="sparql", 
  query_name="publication_normalized",
  query_config=empty_yaml,
  data_endpoint=fuseki_url
)

while True:
  job = s.get_status(j.id)
  if job.done:
    print(job.result)
    print(job.error)
