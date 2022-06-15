from concurrent import futures
import tempfile
import yaml
import os

import defoe_service_pb2_grpc
import defoe_service_pb2
import grpc

import importlib
from pyspark import SparkContext, SparkConf

num_cores = 34
executor_memory = "12g"
driver_memory = "12g"


class DefoeService(defoe_service_pb2_grpc.DefoeServicer):

  def SubmitJob(self, req, context):
    
    result = run_job(req.model_name, req.query_name, req.endpoint)
    return defoe_service_pb2.SubmitResponse(success=True, result=result)


def run_job(model_name, query_name, endpoint):
    root_module = "defoe"
    setup_module = "setup"
    query_config_file = None # not yet supported
    
    setup = importlib.import_module(root_module +
                                    "." +
                                    model_name +
                                    "." +
                                    setup_module)
    query = importlib.import_module(query_name)
    
    conf = SparkConf()
    conf.setAppName(model_name)
    conf.set("spark.cores.max", num_cores)
    conf.set("spark.executor.memory", executor_memory)
    conf.set("spark.driver.memory", driver_memory)
    
    context = SparkContext(conf=conf)
    log = context._jvm.org.apache.log4j.LogManager.getLogger(__name__)  # pylint: disable=protected-access
    
    # Note this skips some checks.
    ok_data = setup.endpoint_to_object(endpoint, context)
    results = query.do_query(ok_data, query_config_file, log, context)
    return yaml.safe_dump(dict(results))


def start_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    defoe_service_pb2_grpc.add_DefoeServicer_to_server(DefoeService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    start_server()

