# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from . import defoe_pb2 as defoe__pb2


class DefoeStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A defoe_grpc.Channel.
        """
        self.submit_job = channel.unary_unary(
                '/defoe.Defoe/submit_job',
                request_serializer=defoe__pb2.JobSubmitRequest.SerializeToString,
                response_deserializer=defoe__pb2.JobSubmitResponse.FromString,
                )
        self.get_job = channel.unary_unary(
                '/defoe.Defoe/get_job',
                request_serializer=defoe__pb2.JobRequest.SerializeToString,
                response_deserializer=defoe__pb2.JobResponse.FromString,
                )


class DefoeServicer(object):
    """Missing associated documentation comment in .proto file."""

    def submit_job(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def get_job(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_DefoeServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'submit_job': grpc.unary_unary_rpc_method_handler(
                    servicer.submit_job,
                    request_deserializer=defoe__pb2.JobSubmitRequest.FromString,
                    response_serializer=defoe__pb2.JobSubmitResponse.SerializeToString,
            ),
            'get_job': grpc.unary_unary_rpc_method_handler(
                    servicer.get_job,
                    request_deserializer=defoe__pb2.JobRequest.FromString,
                    response_serializer=defoe__pb2.JobResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'defoe.Defoe', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class Defoe(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def submit_job(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/defoe.Defoe/submit_job',
            defoe__pb2.JobSubmitRequest.SerializeToString,
            defoe__pb2.JobSubmitResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def get_job(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/defoe.Defoe/get_job',
            defoe__pb2.JobRequest.SerializeToString,
            defoe__pb2.JobResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)