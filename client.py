from concurrent import futures

from google.protobuf.json_format import MessageToJson
import defoe_service_pb2_grpc
import defoe_service_pb2
import grpc

id = "wpa123"

def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = defoe_service_pb2_grpc.DefoeStub(channel)
        
        # Submission
        req = defoe_service_pb2.SubmitRequest(
        id=id, 
        model_name="sparql", 
        query_name="defoe.sparql.queries.publication_normalized", 
        data_endpoint="http://localhost:3030/total_eb/sparql"
        )
        resp = stub.SubmitJob(req)
        print(MessageToJson(resp))
        
        # Status Check
        req1 = defoe_service_pb2.StatusRequest(
        id=id,
        )
        resp1 = stub.GetStatus(req1)
        print(MessageToJson(resp1))

run();

