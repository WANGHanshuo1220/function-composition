import boto3
import threading
import time

import grpc
import message_pb2 as pb2
import message_pb2_grpc as pb2_grpc

from concurrent import futures

class FC_NodeComm(pb2_grpc.NodeCommServicer):
    def __init__(self) -> None:
        self.total_steps = 5
        self.next_step = 1
        self.perf = []

    def FC_NodeComm(self, request, context):
        request_step = request.step
        step_finished = request.finished
        step_perf = request.exec_time
        process_ = False
        if request_step == self.next_step and not step_finished:
            print("recieve a request from step", request_step)
            process_ = True
            return pb2.ReplyInfo(process=process_, exit=False)
        elif request_step == self.next_step and step_finished:
            print("recieve a finished request from step", request_step)
            self.perf.append(step_perf)
            if self.next_step == self.total_steps:
                total_time = 0.0
                for step, perf in enumerate(self.perf):
                    print(f"step {step+1} execution time = {perf:.5f}s")
                    total_time += perf
                print(f"total exection time = {total_time:.5f}s")
            else:
                self.next_step += 1
            return pb2.ReplyInfo(exit=True)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    pb2_grpc.add_NodeCommServicer_to_server(FC_NodeComm(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Server started")

    try:
        while 1:
            time.sleep(10)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    serve()