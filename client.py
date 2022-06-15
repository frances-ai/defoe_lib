from concurrent import futures

import defoe_service_pb2_grpc
import defoe_service_pb2
import grpc


def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = defoe_service_pb2_grpc.DefoeStub(channel)
        req = defoe_service_pb2.SubmitRequest(id='wpa1', model_name="sparql", query_name="defoe.sparql.queries.publication_normalized", endpoint="http://localhost:3030/total_eb/sparql")
        response = stub.SubmitJob(req)
        print("Defoe client received: " + response.result)

run();

