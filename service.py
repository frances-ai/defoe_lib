from concurrent import futures
import threading
import tempfile
import yaml
import os

import defoe_service_pb2_grpc
import defoe_service_pb2
import grpc

import importlib
from pyspark.sql import SparkSession

num_cores = 34
executor_memory = "6g"
driver_memory = "6g"

empty_yaml = "--- !!str"
spark_url = "local[1]"

jobs = {}


class Job:
  def __init__(self, id):
    self.id = id
    self.result = ""
    self.done = False
    self.error = ""
    self._lock = threading.Lock()


class DefoeService(defoe_service_pb2_grpc.DefoeServicer):

  def SubmitJob(self, req, context):
    if req.id in jobs:
      return defoe_service_pb2.SubmitResponse(error="job id already exists")
    jobs[req.id] = Job(req.id)
    
    if req.query_config is None or req.query_config == "":
      req.query_config = empty_yaml
    
    args = (req.id, req.model_name, req.query_name, req.query_config, req.data_endpoint)
    work = threading.Thread(target=self.run_job, args=args)
    work.start()
    
    return defoe_service_pb2.SubmitResponse(id=req.id)

  def GetStatus(self, req, context):
    if req.id not in jobs:
      return defoe_service_pb2.StatusResponse(error="job id not found")
    
    job = jobs[req.id]
    with job._lock:
      return defoe_service_pb2.StatusResponse(done=job.done, result=job.result, error=job.error)

  def run_job(self, id, model_name, query_name, query_config, data_endpoint):
      root_module = "defoe"
      setup_module = "setup"
      setup = importlib.import_module(root_module +
                                      "." +
                                      model_name +
                                      "." +
                                      setup_module)
      query = importlib.import_module(query_name)
      
      spark = self.get_spark_context()
      log = spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)  # pylint: disable=protected-access
      
      # Note this skips some checks.
      job = jobs[id]
      result = None
      error = None
      try:
        ok_data = setup.endpoint_to_object(data_endpoint, spark)
        result = query.do_query(ok_data, job, query_config, log, spark)
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
          .master(spark_url) \
          .config("spark.cores.max", num_cores) \
          .config("spark.executor.memory", executor_memory) \
          .config("spark.driver.memory", driver_memory) \
          .getOrCreate()

def start_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    defoe_service_pb2_grpc.add_DefoeServicer_to_server(DefoeService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    start_server()

