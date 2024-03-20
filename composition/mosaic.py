import os
import time
import cv2 as cv2
import boto3

import grpc
import message_pb2 as pb2
import message_pb2_grpc as pb2_grpc

STEP = 5

ENV_WORKDIR="/home/ubuntu/fc/"
bucket_name = "function-composition"
preprocessed_object_key = "recognition/image1/preprocessed.jpg"
img_download_path = "porn_dl.jpg"
mosaic_path = "mosaiced.jpg"
mosiac_object_key = "recognition/image1/mosiac.jpg"

s3_client = boto3.client('s3')

def mosaic():
    try:
        s3_client.download_file(bucket_name, preprocessed_object_key, img_download_path)
        print("Image downloaded successfully from S3")
    except Exception as e:
        print(f"Error downloading image from S3: {e}")
    
    img = cv2.imread(img_download_path, 1)
    img = cv2.resize(img, None, fx=0.1, fy=0.1)
    height, width, deep = img.shape
    mosaic_height = 8
    for m in range(height - mosaic_height):
        for n in range(width - mosaic_height):
            if m % mosaic_height == 0 and n % mosaic_height == 0:
                for i in range(mosaic_height):
                    for j in range(mosaic_height):
                        b, g, r = img[m, n]
                        img[m + i, n + j] = (b, g, r)
    
    cv2.imwrite(mosaic_path, img)
    
    try:
        s3_client.upload_file(mosaic_path, bucket_name, mosiac_object_key)
        print("Mosaiced img uploaded successfully to S3")
    except Exception as e:
        print(f"Error uploading Mdata to S3: {e}")

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
                    success = mosaic()
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
        pass