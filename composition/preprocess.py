import boto3
import os
import time
import os
from wand.image import Image
from botocore.exceptions import ClientError

import grpc
import message_pb2 as pb2
import message_pb2_grpc as pb2_grpc

STEP = 3

bucket_name = "function-composition"
mdata_object_key = "recognition/image1/mdata.json"
img_object_key = "recognition/image1/image.jpg"
img_download_path = "porn.jpg"
preprocessed_path = "preprocessed.jpg"
preprocessed_object_key = "recognition/image1/preprocessed.jpg"
json_mdata_path = 'image_metadata.json'

s3_client = boto3.client('s3')

def preprocessing(local):
    process = True
    if not local:
        try:
            s3_client.head_object(Bucket=bucket_name, Key=mdata_object_key)
        except ClientError as e:
            process = False
    else:
        if not os.path.exists(json_mdata_path):
            process = False

    if process:
        if not local:
            try:
                s3_client.download_file(bucket_name, img_object_key, img_download_path)
                print("Image downloaded successfully from S3")
            except Exception as e:
                print(f"Error downloading image from S3: {e}")
        else:
            print("Get img from local")

        with Image(filename=img_download_path) as img:
            img.resize(224, 224)
            preprocessed_img = img.clone()

            with open(preprocessed_path, 'wb') as f:
                preprocessed_img.save(file=f)

        if not local:
            try:
                s3_client.upload_file(preprocessed_path, bucket_name, preprocessed_object_key)
                print("Preprocessed img uploaded successfully to S3")
            except Exception as e:
                print(f"Error uploading Mdata to S3: {e}")
        else:
            print("Save img locally")

    return True
    
if __name__ == "__main__":
    host_ip = os.environ.get("FC_HOST_IP")
    channel = grpc.insecure_channel(host_ip + ':50051')
    stub = pb2_grpc.NodeCommStub(channel)
    
    try:
        while True:
            try:
                response = stub.FC_NodeComm(pb2.RequestInfo(step=STEP, finished=False))
                if response.process:
                    print("Received message from server:", response.process)
                    local = response.local

                    # do the job
                    start_time = time.time()
                    success = preprocessing(local)
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