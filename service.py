from concurrent import futures
import threading
import traceback
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

  def get_pre_computed_queries(self):
    current_dir = os.path.realpath(os.path.dirname(__file__))
    return {"publication_normalized": current_dir + "/precomputedResult/publication_normalized.yml"}

  def submit_job(self, job_id, model_name, query_name, query_config):
    if job_id in jobs:
      raise ValueError("job id already exists")
    print(type(job_id))
    jobs[job_id] = Job(job_id)

    if query_config is None or query_config == "":
      query_config = {}

    args = (job_id, model_name, query_name, query_config)
    work = threading.Thread(target=self.run_job, args=args)
    work.start()

    return jobs[job_id]

  def get_status(self, job_id):
    print(jobs)
    print(job_id)
    test_jobs = {
        job_id: -1
    }
    print(test_jobs)
    if job_id not in jobs:
      raise ValueError("job id not found")
    return jobs[job_id]

  def run_job(self, id, model_name, query_name, query_config):
      job = self.get_status(id)

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

      if query_name in self.get_pre_computed_queries():
          with job._lock:
              jobs[id].done = True
              jobs[id].result = self.get_pre_computed_queries()[query_name]
              return

      query = model.get_queries()[query_name]

      spark = self.get_spark_context()
      log = spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)  # pylint: disable=protected-access

      # Note this skips some checks.
      result = None
      error = None
      try:
        ok_data = model.endpoint_to_object(self.config.fuseki_url, spark)
        result = query(ok_data, query_config, log, spark)
      except Exception as e:
        print("Job " + id + " threw an exception")
        print(traceback.format_exc())
        error = e

      with job._lock:
        jobs[id].done = True
        jobs[id].error = repr(error)
        if result != None:
            os.makedirs(os.path.dirname(query_config["result_file_path"]), exist_ok=True)
            with open(query_config["result_file_path"], "w") as f:
                f.write(yaml.safe_dump(dict(result)))
                jobs[id].result = query_config["result_file_path"]

  def get_spark_context(self):
    build = SparkSession \
          .builder \
          .master(self.config.spark_url) \
          .config("spark.cores.max", num_cores) \
          .config("spark.executor.memory", executor_memory) \
          .config("spark.driver.memory", driver_memory) \
          .config("spark.rpc.message.maxSize", max_message_size) \
          .config("spark.driver.maxResultSize", max_result_size)

    remote_mode = self.config.remote != None

    if remote_mode:
      build = build \
        .config("spark.pyspark.python", "./ENV/bin/python") \
        .config("spark.pyspark.driver.python", "./ENV/bin/python") \
        .config("spark.archives", self.config.remote.environment + "#environment") \
        .config("spark.driver.host", self.config.remote.driver_host) \
        .config("spark.driver.bindAddress", self.config.remote.driver_host) \
        .config("spark.blockManager.port", "10025") \
        .config("spark.driver.blockManager.port", "10026") \
        .config("spark.driver.port", "10027")

    ss = build.getOrCreate()
    if remote_mode:
      ss.sparkContext.addPyFile(self.config.remote.module)
    return ss


