import boto3
import os
import threading
import time

import grpc
import message_pb2 as pb2
import message_pb2_grpc as pb2_grpc

from concurrent import futures


class upload:
    def __init__(self) -> None:
        self.step = 1
        self.bucket_name = "function-composition"
        self.image_path = "porn.jpg"
        self.img_object_key = "recognition/image1/image.jpg"
        self.s3_client = boto3.client('s3')

    def upload(self, local):
        ################################# upload image to s3 #################################
        if not local:
            try:
                self.s3_client.upload_file(self.image_path, self.bucket_name, self.img_object_key)
                print("Image uploaded successfully to S3")
            except Exception as e:
                print(f"Error uploading image to S3: {e}")
        else:
            pass
        
        return True

    def run(self):
        host_ip = os.environ.get("FC_HOST_IP")
        channel = grpc.insecure_channel(str(host_ip) + ':50051')
        stub = pb2_grpc.NodeCommStub(channel)

        try:
            while True:
                try:
                    response = stub.FC_NodeComm(pb2.RequestInfo(step=self.step, finished=False))
                    if response.process:
                        print("Received message from server:", response.process)
                        local = response.local

                        # do the job
                        start_time = time.time()
                        success = self.upload(local)
                        end_time = time.time()

                        # send job finished to master
                        res = stub.FC_NodeComm(pb2.RequestInfo(
                            step=self.step, 
                            finished=success, 
                            exec_time=end_time-start_time
                            ))

                        if res.exit:
                            return

                except Exception as e:
                    # print("An error occured: ", e)
                    continue
        except KeyboardInterrupt:
            exit(0)