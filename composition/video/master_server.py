import time
import os
from concurrent import futures
import sys
import subprocess
import boto3

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
        self.e2e_e = []


    def Save_perfs(self, workers_perf, e2e_e):
        self.e2e_e.append(e2e_e)
        this_step = []
        for worker_perf in workers_perf:
            this_worker_perf = []
            for perf in worker_perf.perfs:
                this_worker_perf.append(perf)
            this_worker = {worker_perf.worker_id: this_worker_perf}
            this_step.append(this_worker)
        self.perfs.append(this_step)


    def Show_perfs(self):
        step_max_time = []
        for step in range(len(self.perfs)):
            print(f"In step {step}:")
            max_total_time = 0.0
            worker_id = 0
            for workers_perf in self.perfs[step]:
                total_time = workers_perf[worker_id][0]+ workers_perf[worker_id][1] 
                if total_time > max_total_time:
                    max_total_time = total_time
                print(f"    Worker {worker_id}: total_time = "
                      f"{(total_time):.4f}s: "
                      f"(C = {workers_perf[worker_id][0]:.4f}s, "
                      f"S = {workers_perf[worker_id][1]:.4f}s)")
                worker_id += 1
            step_max_time.append(max_total_time)
        return step_max_time, self.e2e_e

    
    def Run_DAG(self, DAG_name, parallel):
        dag = self.DAG_repo_.Get_DAG(DAG_name)
        # print(f"DAG name = {DAG_name}, DAG info = {dag}")

        for step in range(dag["steps"]):
            try:
                while True:
                    # print(f"Send step {step} at time {time.time()}")
                    response = self.stub.Func_Exec(pb2.RequestInfo(step = step,
                                                                   DAG_name = DAG_name,
                                                                   parallel = parallel))
                    if response.success == True:
                        # print(f"Success {DAG_name} step {step}!")
                        self.Save_perfs(response.workers_perf, response.cap)
                        break
                    else:
                        print(f"Retry {DAG_name} step {step} ...")

            except Exception as e:
                print(f"DAG {DAG_name} step {step} execution error with:\n{e}")


def Clean_local():
    try:
        with open('/dev/null', 'w') as devnull:
            subprocess.call('rm -r /tmp/*.mp4 /tmp/mdata.json /tmp/detected_* '
                            '/tmp/frame_* /tmp/chunk_* /tmp/worker_*', 
                            shell = True,
                            stdout = devnull,
                            stderr = devnull)
    except:
        pass

    
def Clean_s3(video_type):
    s3_bucket_name = "function-composition"
    s3_chunk_key = "video/"+ video_type + "/chunks/"
    s3_mdata_key = "video/"+ video_type + "/mdata.json"
    s3_frame_key = "video/" + video_type + "/frames/"
    s3_detection_key = "video/" + video_type + "/frame_detection/"

    
    s3_client = boto3.client('s3')
    try:
        s3_client.delete_object(Bucket = s3_bucket_name, 
                                     Key    = s3_mdata_key)

        try:
            response = s3_client.list_objects_v2(Bucket = s3_bucket_name,
                                                            Prefix = s3_chunk_key)
        except Exception as e:
            print("Delete error with", e)

        files_in_folder = response["Contents"]
        files_to_delete = []
        # We will create Key array to pass to delete_objects function
        for f in files_in_folder:
            files_to_delete.append({"Key": f["Key"]})

        # This will delete all files in a folder
        response = s3_client.delete_objects(
            Bucket = s3_bucket_name, Delete={"Objects": files_to_delete}
        )
    except:
            pass

    try:
        try:
            response = s3_client.list_objects_v2(Bucket = s3_bucket_name,
                                                            Prefix = s3_frame_key)
        except Exception as e:
            print("Delete error with", e)

        files_in_folder = response["Contents"]
        files_to_delete = []
        # We will create Key array to pass to delete_objects function
        for f in files_in_folder:
            files_to_delete.append({"Key": f["Key"]})

        # This will delete all files in a folder
        response = s3_client.delete_objects(
            Bucket = s3_bucket_name, Delete={"Objects": files_to_delete}
        )
    except:
        pass


    try:
        response = s3_client.list_objects_v2(Bucket = s3_bucket_name,
                                                        Prefix = s3_detection_key)
        if 'Contents' in response:
            files_in_folder = response["Contents"]
            files_to_delete = []
            # We will create Key array to pass to delete_objects function
            for f in files_in_folder:
                files_to_delete.append({"Key": f["Key"]})

            # This will delete all files in a folder
            s3_client.delete_objects(
                Bucket = s3_bucket_name, Delete={"Objects": files_to_delete}
            )
    except Exception as e:
        print("Clean S3 error with", e)
        pass


if __name__ == "__main__":
    video_type = "small"
    Clean_local()
    Clean_s3(video_type)

    parallels = [1, 2, 3, 6]
    # parallels = [1]

    for parallel in parallels:
        ms = master_server()

        print("")
        print(f"******************  {parallel}  *********************")

        start_time = time.time()
        ms.Run_DAG("video", parallel)
        end_time = time.time()

        e2e = end_time - start_time
        print(f"total exec time = {e2e:.4f} s")
        print("================================")

        steps_max_step, e2e_e = ms.Show_perfs()

        print("================================")
        total_exec_time = 0.0
        for step, t in enumerate(steps_max_step):
            total_exec_time += t
            print(f"Step {step} max time = {t:.4f}s"
                  f" e2e_e = {e2e_e[step]:.4f}s"
                  f" delta = {(e2e_e[step]-t):.4f}s")
    
        print(f"Total exec time = {total_exec_time:.4f}s")
        print(f"Else time = {(e2e-total_exec_time):.4f}s")
