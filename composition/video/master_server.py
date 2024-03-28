import time
import os
from concurrent import futures
import sys

import grpc
import message_pb2 as pb2
import message_pb2_grpc as pb2_grpc

INVALID_DAG = "INVALID_DAG"
INVALID_STEP = -1

class DAG_repo:
    def __init__(self) -> None:
        self.DAGs = {
            "video":{
                "steps": 3
            }
        }

    
    def Get_DAG(self, DAG_name):
        return self.DAGs[DAG_name]

    
    def Register_DAG(self):
        pass


class master_server:
    def __init__(self) -> None:
        # self.host_ip = os.environ.get("FC_HOST_IP")
        self.host_ip = "localhost"
        self.channel = grpc.insecure_channel(str(self.host_ip) + ':50051')
        self.stub = pb2_grpc.NodeCommStub(self.channel)

        self.DAG_repo_ = DAG_repo()

        self.in_DAG = INVALID_DAG
        self.step = INVALID_STEP
        
        self.perfs = []


    def Save_perfs(self, workers_perf):
        this_step = []
        for worker_perf in workers_perf:
            this_worker_perf = []
            for perf in worker_perf.perfs:
                this_worker_perf.append(perf)
            this_worker = {worker_perf.worker_id: this_worker_perf}
            this_step.append(this_worker)
        self.perfs.append(this_step)


    def Show_perfs(self):
        for step in range(len(self.perfs)):
            print(f"In step {step}:")
            worker_id = 0
            for workers_perf in self.perfs[step]:
                print(f"    Worker {worker_id}: C_time = {workers_perf[worker_id][0]:.8f} s, "
                      f"S_time = {workers_perf[worker_id][1]:.8f} s")
                worker_id += 1


    
    def Run_DAG(self, DAG_name, parallel):
        dag = self.DAG_repo_.Get_DAG(DAG_name)
        print(f"DAG name = {DAG_name}, DAG info = {dag}")

        for step in range(dag["steps"]):
            try:
                while True:
                    response = self.stub.Func_Exec(pb2.RequestInfo(step = step,
                                                                   DAG_name = DAG_name,
                                                                   parallel = parallel))
                    if response.success == True:
                        print(f"Success {DAG_name} step {step}!")
                        self.Save_perfs(response.workers_perf)
                        break
                    else:
                        print(f"Retry {DAG_name} step {step} ...")

            except Exception as e:
                print(f"DAG {DAG_name} step {step} execution error with:\n{e}")


if __name__ == "__main__":

    ms = master_server()

    start_time = time.time()
    ms.Run_DAG("video", int(sys.argv[1]))
    end_time = time.time()

    exec_time = end_time - start_time
    print(f"total exec time = {exec_time:.8f} s")
    print("================================")
    ms.Show_perfs()