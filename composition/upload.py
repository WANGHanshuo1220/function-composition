import boto3
import threading
import time

import grpc
import message_pb2 as pb2
import message_pb2_grpc as pb2_grpc

from concurrent import futures

STEP = 1

ENV_WORKDIR="/home/ubuntu/fc/"
bucket_name = "function-composition"
image_path = "porn.jpg"
img_object_key = "recognition/image1/image.jpg"

s3_client = boto3.client('s3')

def upload():
    ################################# upload image to s3 #################################
    try:
        s3_client.upload_file(image_path, bucket_name, img_object_key)
        print("Image uploaded successfully to S3")
    except Exception as e:
        print(f"Error uploading image to S3: {e}")
    
    return True

if __name__ == "__main__":
    channel = grpc.insecure_channel('localhost:50051')
    stub = pb2_grpc.NodeCommStub(channel)
    
    try:
        while True:
            try:
                response = stub.FC_NodeComm(pb2.RequestInfo(step=STEP, finished=False))
                if response.process:
                    print("Received message from server:", response.process)

                    # do the job
                    start_time = time.time()
                    success = upload()
                    end_time = time.time()

                    # send job finished to master
                    res = stub.FC_NodeComm(pb2.RequestInfo(
                        step=STEP, 
                        finished=success, 
                        exec_time=end_time-start_time
                        ))

                    if res.exit:
                        exit(0)

            except Exception as e:
                continue
    except KeyboardInterrupt:
        exit(0)