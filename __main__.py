from defoe_lib.config import DefoeConfig
from defoe_lib.service import DefoeService

empty_yaml = "--- !!str"


c = DefoeConfig(
  spark_url="local[1]",
  fuseki_url="http://localhost:3030/total_eb/sparql",
)
s = DefoeService(c)
j = s.submit_job(
  job_id="wpa123",
  model_name="sparql", 
  query_name="publication_normalized",
  query_config=empty_yaml
)

while True:
  job = s.get_status(j.id)
  if job.done:
    print(job.result)
    print(job.error)
