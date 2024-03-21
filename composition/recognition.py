import json
import os
import time
import numpy as np
from tensorflow.keras.models import load_model
from tensorflow.keras.preprocessing import image
import cv2 as cv2
import boto3

import grpc
import message_pb2 as pb2
import message_pb2_grpc as pb2_grpc

STEP = 4

bucket_name = "function-composition"
preprocessed_object_key = "recognition/image1/preprocessed.jpg"
img_download_path = "preprocessed.jpg"
model_object_key = "recognition/model.h5"
model_download_path = "model.h5"
json_illegal_path = "is_illegal.json"
illegal_object_key = "recognition/image1/illegal.json"

s3_client = boto3.client('s3')

def recognition(local):
    if not local:
        try:
            s3_client.download_file(bucket_name, preprocessed_object_key, img_download_path)
            print("Image downloaded successfully from S3")
        except Exception as e:
            print(f"Error downloading image from S3: {e}")

        try:
            s3_client.download_file(bucket_name, model_object_key, model_download_path)
            print("Model downloaded successfully from S3")
        except Exception as e:
            print(f"Error downloading model from S3: {e}")
    else:
        print("Get img and model from local")

    model = load_model(model_download_path)
    SIZE = (224, 224)

    img = image.load_img(img_download_path, target_size=SIZE)
    input_x = image.img_to_array(img)
    input_x = np.expand_dims(input_x, axis=0)
    preds = model.predict(input_x)

    illegal = False
    if preds[0][0] > 0.95:
        illegal = True
    print("illegal = ", illegal)

    data = {"Is_illegal": illegal}
    with open(json_illegal_path, 'w') as json_file:
        json.dump(data, json_file)

    if not local:
        try:
            s3_client.upload_file(json_illegal_path, bucket_name, illegal_object_key)
            print("Illegal json uploaded successfully to S3")
        except Exception as e:
            print(f"Error uploading illegal json to S3: {e}")
    else:
        print("Save iilegal json locally")

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
                    success = recognition(local)
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