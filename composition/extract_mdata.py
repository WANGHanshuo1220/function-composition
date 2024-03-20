import json
import time
import boto3
from wand.image import Image

import grpc
import message_pb2 as pb2
import message_pb2_grpc as pb2_grpc

STEP = 2

bucket_name = "function-composition"
img_object_key = "recognition/image1/image.jpg"
img_download_path = "porn_dl.jpg"
json_mdata_path = 'image_metadata.json'
mdata_object_key = "recognition/image1/mdata.json"

s3_client = boto3.client('s3')

def extract():
    try:
        s3_client.download_file(bucket_name, img_object_key, img_download_path)
        print("Image downloaded successfully from S3")
    except Exception as e:
        print(f"Error downloading image from S3: {e}")

    with Image(filename=img_download_path) as img:
        metadata = {
            "format": img.format,
            "width": img.width,
            "height": img.height,
            "resolution": img.resolution,
        }

    with open(json_mdata_path, 'w') as json_file:
            json.dump(metadata, json_file, indent=4)

    try:
        s3_client.upload_file(json_mdata_path, bucket_name, mdata_object_key)
        print("Mdata uploaded successfully to S3")
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
                    success = extract()
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