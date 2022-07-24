from concurrent import futures
import threading
import tempfile
import yaml
import os

import importlib
from pyspark.sql import SparkSession

import defoe_lib.defoe.sparql as sparql

num_cores = 34
executor_memory = "6g"
driver_memory = "6g"
max_message_size = 2047 # Max allowed message size
max_result_size = 0 # Unlimited result size

models = {
  "sparql": sparql.Model(),
}

empty_yaml = "--- !!str"
fuseki_url = "http://localhost:3030/total_eb/sparql"

jobs = {}


class Job:
  def __init__(self, id):
    self.id = id
    self.result = ""
    self.done = False
    self.error = ""
    self._lock = threading.Lock()


class DefoeService:
  def __init__(self, config):
    self.config = config

  def submit_job(self, job_id, model_name, query_name, query_config, data_endpoint):
    if job_id in jobs:
      raise ValueError("job id already exists")
    jobs[job_id] = Job(job_id)
    
    if query_config is None or query_config == "":
      query_config = empty_yaml
    
    args = (job_id, model_name, query_name, query_config, data_endpoint)
    work = threading.Thread(target=self.run_job, args=args)
    work.start()
  
    return jobs[job_id]

  def get_status(self, job_id):
    if job_id not in jobs:
      raise ValueError("job id not found")
    return jobs[job_id]

  def run_job(self, id, model_name, query_name, query_config, data_endpoint):
      if model_name not in models:
        with job._lock:
          jobs[id].done = True
          jobs[id].error = "model not found"
          return
      model = models[model_name]
      
      if query_name not in model.get_queries():
        with job._lock:
          jobs[id].done = True
          jobs[id].error = "query not found"
          return
      query = model.get_queries()[query_name]
      
      spark = self.get_spark_context()
      log = spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)  # pylint: disable=protected-access
      
      # Note this skips some checks.
      job = jobs[id]
      result = None
      error = None
      try:
        ok_data = model.endpoint_to_object(data_endpoint, spark)
        result = query(ok_data, job, query_config, log, spark)
      except Exception as e:
        error = e
      
      with job._lock:
        jobs[id].done = True
        jobs[id].error = repr(error)
        if result != None:
          jobs[id].result = yaml.safe_dump(dict(result))

  def get_spark_context(self):
    return SparkSession \
          .builder \
          .master(self.config.spark_url) \
          .config("spark.cores.max", num_cores) \
          .config("spark.executor.memory", executor_memory) \
          .config("spark.driver.memory", driver_memory) \
          .config("spark.rpc.message.maxSize", max_message_size) \
          .config("spark.driver.maxResultSize", max_result_size) \
          .getOrCreate()


if __name__ == '__main__':
    c = DefoeConfig("local[1]")
    s = DefoeService(c)
    j = s.submit_job(
        job_id="wpa123",
        model_name="sparql", 
        query_name="defoe.sparql.queries.publication_normalized",
        query_config=empty_yaml,
        data_endpoint=fuseki_url
    )
    
    while True:
      res = s.get_status(j.id)
      print(res.result)
