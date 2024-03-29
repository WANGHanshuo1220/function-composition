import os
import time

import grpc
import message_pb2 as pb2
import message_pb2_grpc as pb2_grpc

from concurrent import futures
import multiprocessing
from multiprocessing import Process
import boto3

from split import Run_split
from extract import Run_extract
from classify import Run_classify

# import warnings
# warnings.filterwarnings("ignore", category=ResourceWarning)

PARALLEL = 4

class DAG_repo:
    def __init__(self) -> None:
        self.DAGs = {
            "video":{
                # "step": [1, PARALLEL, PARALLEL],
                "func": ["Run_split", "Run_extract", "Run_classify"]
            }
        }
        

    
    def Get_DAG(self, DAG_name):
        return self.DAGs[DAG_name]

    
    def Register_DAG(self):
        pass


class Func_Exec(pb2_grpc.NodeCommServicer):
    def __init__(self, s3_client)-> None:
        self.total_steps = 3
        self.next_step = 1
        self.DAG_repo_ = DAG_repo()
        self.s3_client = s3_client

        # Perf metrics
        self.func_start_time = []
        self.func_exec_time = []
        self.s3_time = []


    def Func_Exec(self, request, context):
        request_step = request.step
        # print("==============================================================")
        # print(f"Recieve step {request_step} at time {time.time()}")
        DAG_name = request.DAG_name
        if request_step == 0:
            parallel = 1
        else:
            parallel = request.parallel

        dag = self.DAG_repo_.Get_DAG(DAG_name)

        function = globals()[dag["func"][request_step]]

        success = True
        reply = pb2.ReplyInfo()
        try:
            ths = []
            manager = multiprocessing.Manager()
            return_dicts = [manager.list() for _ in range(parallel)]
            s1 = time.time()
            for th in range(parallel):
                ths.append(Process(
                    target = function,
                    args = (th, request.parallel, self.s3_client, return_dicts[th], )
                ))
            s2 = time.time()
            if s2-s1 > 0.5:
                print(f"Step {request_step}, Parallel {request.parallel}: "
                      f"prepare time = {(s2-s1):.4f}s")
            
            s1 = time.time()
            for t in range(len(ths)):
                ths[t].start()
            for t in range(len(ths)):
                ths[t].join()
            s2 = time.time()
            cap = s2 - s1
            reply.cap = cap

            for worker_id, return_dict in enumerate(return_dicts):
                worker = reply.workers_perf.add()
                worker.worker_id = worker_id
                for value in return_dict:
                    worker.perfs.append(value)
                

        except Exception as e:
            # print(f"DAG {DAG_name} step {request_step} execution error with:\n{e}")
            success = False

        reply.success = success
        return reply


class worker_server:
    def __init__(self) -> None:
        s3_client = boto3.client('s3')
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        pb2_grpc.add_NodeCommServicer_to_server(Func_Exec(s3_client), self.server)
        self.server.add_insecure_port('[::]:50051')


    def Stop(self):
        self.server.stop(2)
        print("Server stopped successfully")

    
    def Run(self):
        self.server.start()
        print("Server started")

        try:
            while 1:
                time.sleep(10)
        except KeyboardInterrupt:
            self.Stop()


if __name__ == "__main__":
    master = worker_server()
    master.Run()