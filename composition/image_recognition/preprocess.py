import boto3
import os
import time
import os
from wand.image import Image
from botocore.exceptions import ClientError

import grpc
import message_pb2 as pb2
import message_pb2_grpc as pb2_grpc


class preprocessing:
    def __init__(self) -> None:
        self.step = 3
        self.bucket_name = "function-composition"
        self.mdata_object_key = "recognition/image1/mdata.json"
        self.img_object_key = "recognition/image1/image.jpg"
        self.img_download_path = "porn.jpg"
        self.preprocessed_path = "preprocessed.jpg"
        self.preprocessed_object_key = "recognition/image1/preprocessed.jpg"
        self.json_mdata_path = 'image_metadata.json'
        self.s3_client = boto3.client('s3')
        
    
    def preprocessing(self, local):
        process = True
        if not local:
            try:
                self.s3_client.head_object(Bucket=self.bucket_name, Key=self.mdata_object_key)
            except ClientError as e:
                process = False
        else:
            if not os.path.exists(self.json_mdata_path):
                process = False
    
        if process:
            if not local:
                try:
                    self.s3_client.download_file(self.bucket_name, self.img_object_key, self.img_download_path)
                    print("Image downloaded successfully from S3")
                except Exception as e:
                    print(f"Error downloading image from S3: {e}")
            else:
                print("Get img from local")
    
            with Image(filename=self.img_download_path) as img:
                img.resize(224, 224)
                preprocessed_img = img.clone()
    
                with open(self.preprocessed_path, 'wb') as f:
                    preprocessed_img.save(file=f)
    
            if not local:
                try:
                    self.s3_client.upload_file(self.preprocessed_path, self.bucket_name, self.preprocessed_object_key)
                    print("Preprocessed img uploaded successfully to S3")
                except Exception as e:
                    print(f"Error uploading Mdata to S3: {e}")
            else:
                print("Save img locally")
    
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
                        success = self.preprocessing(local)
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