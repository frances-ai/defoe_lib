import time
from concurrent import futures
import threading
import traceback
import tempfile
import yaml
import os

from pyspark.sql import SparkSession

import defoe.sparql as sparql
import defoe.hto as hto

num_cores = 34
executor_memory = "16g"
driver_memory = "16g"
max_message_size = 2047  # Max allowed message size
max_result_size = 0  # Unlimited result size

models = {
    "sparql": sparql.Model(),
    "hto": hto
}

jobs = {}


class Job:
    def __init__(self, id):
        self.id = id
        self.result_path = ""
        self.state = "RUNNING"
        self.error = ""
        self._lock = threading.Lock()


def get_spark_context():
    build = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.cores.max", num_cores) \
        .config("spark.executor.memory", executor_memory) \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.rpc.message.maxSize", max_message_size) \
        .config("spark.driver.maxResultSize", max_result_size)

    ss = build.getOrCreate()

    return ss


def get_jobs(job_id):
    if job_id not in jobs:
        raise ValueError("job id not found")

    job = jobs[job_id]
    return job


def run_job(id, model_name, query_name, endpoint, query_config, result_file_path):
    config = {}
    for config_key in query_config:
        if config_key == "target_sentences" or config_key == "exclude_words":
            config[config_key] = query_config[config_key].split(",")
        else:
            config[config_key] = query_config[config_key]

    job = get_jobs(id)

    if model_name not in models:
        with job._lock:
            jobs[id].state = "ERROR"
            jobs[id].error = "model not found"
            return
    model = models[model_name]

    if query_name not in model.get_queries():
        with job._lock:
            jobs[id].state = "ERROR"
            jobs[id].error = "query not found"
            return

    query = model.get_queries()[query_name]
    jobs[id].start_time = time.time()
    print("job started, start time: ", jobs[id].start_time)
    spark = get_spark_context()
    log = spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)  # pylint: disable=protected-access

    # Note this skips some checks.
    result = None
    error = None

    try:
        print("sparql endpoint: %s", endpoint)
        if model_name == "hto":
            collection_name = config["collection"]
            source = config["source"]
            ok_data = model.get_hto_df(endpoint, collection_name, source, spark)
        else:
            ok_data = model.endpoint_to_object(endpoint, spark)
        result = query(ok_data, config, log, spark)
    except Exception as e:
        print("Job " + id + " threw an exception")
        print(traceback.format_exc())
        error = e

    with job._lock:
        if error is not None:
            jobs[id].error = repr(error)
            jobs[id].state = "ERROR"
        else:
            jobs[id].state = "DONE"
            if result != None:
                duration = time.time() - jobs[id].start_time
                print("job finished!. it starts from", jobs[id].start_time, ", it takes ", duration)
                jobs[id].duration = duration
                os.makedirs(os.path.dirname(result_file_path), exist_ok=True)
                with open(result_file_path, "w") as f:
                    f.write(yaml.safe_dump(dict(result)))
                    jobs[id].result = result_file_path


def submit_job(job_id, model_name, query_name, endpoint, query_config, result_file_path):
    if job_id in jobs:
        raise ValueError("job id already exists")
    jobs[job_id] = Job(job_id)

    if query_config is None or query_config == "":
        query_config = {}

    args = (job_id, model_name, query_name, endpoint, query_config, result_file_path)
    work = threading.Thread(target=run_job, args=args)
    work.start()

    return jobs[job_id]
