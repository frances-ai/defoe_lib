import grpc
from . import defoe_pb2_grpc
from . import defoe_pb2

from concurrent import futures
import logging
import defoe_service


class DefoeServer(defoe_pb2_grpc.DefoeServicer):

    def submit_job(self, request, context):
        defoe_service.submit_job(request.job_id, request.model_name,
                                 request.query_name, request.endpoint, request.query_config, request.result_file_path)
        return defoe_pb2.JobSubmitResponse(job_id=request.job_id)

    def get_job(self, request, context):
        job = defoe_service.get_jobs(request.job_id)
        return defoe_pb2.JobResponse(job_id=job.id, state=job.state, result_file_path=job.result_path, error=job.error)


def serve():
    port = '5052'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    defoe_pb2_grpc.add_DefoeServicer_to_server(DefoeServer(), server)
    server.add_insecure_port('[::]:' + port)
    server.start()
    print("Defoe Server started, listening on " + port)
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()
