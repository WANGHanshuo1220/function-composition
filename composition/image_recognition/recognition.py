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


class recongition:
    def __init__(self) -> None:
        self.step = 4
        self.bucket_name = "function-composition"
        self.preprocessed_object_key = "recognition/image1/preprocessed.jpg"
        self.img_download_path = "preprocessed.jpg"
        self.model_object_key = "recognition/model.h5"
        self.model_download_path = "model.h5"
        self.json_illegal_path = "is_illegal.json"
        self.illegal_object_key = "recognition/image1/illegal.json"
        self.s3_client = boto3.client('s3')
        
    
    def recognition(self, local):
        if not local:
            try:
                self.s3_client.download_file(self.bucket_name, self.preprocessed_object_key, self.img_download_path)
                print("Image downloaded successfully from S3")
            except Exception as e:
                print(f"Error downloading image from S3: {e}")
    
            try:
                self.s3_client.download_file(self.bucket_name, self.model_object_key, self.model_download_path)
                print("Model downloaded successfully from S3")
            except Exception as e:
                print(f"Error downloading model from S3: {e}")
        else:
            print("Get img and model from local")
    
        model = load_model(self.model_download_path)
        SIZE = (224, 224)
    
        img = image.load_img(self.img_download_path, target_size=SIZE)
        input_x = image.img_to_array(img)
        input_x = np.expand_dims(input_x, axis=0)
        preds = model.predict(input_x)
    
        illegal = False
        if preds[0][0] > 0.95:
            illegal = True
        print("illegal = ", illegal)
    
        data = {"Is_illegal": illegal}
        with open(self.json_illegal_path, 'w') as json_file:
            json.dump(data, json_file)
    
        if not local:
            try:
                self.s3_client.upload_file(self.json_illegal_path, self.bucket_name, self.illegal_object_key)
                print("Illegal json uploaded successfully to S3")
            except Exception as e:
                print(f"Error uploading illegal json to S3: {e}")
        else:
            print("Save iilegal json locally")
    
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
                        success = self.recognition(local)
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
                    print("An error occured: ", e)
                    # continue
        except KeyboardInterrupt:
            pass