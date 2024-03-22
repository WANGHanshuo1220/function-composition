import os
import time
import cv2 as cv2
import boto3

import grpc
import message_pb2 as pb2
import message_pb2_grpc as pb2_grpc


class mosaic:
    def __init__(self) -> None:
        self.step = 5
        self.ENV_WORKDIR="/home/ubuntu/fc/"
        self.bucket_name = "function-composition"
        self.preprocessed_object_key = "recognition/image1/preprocessed.jpg"
        self.img_download_path = "preprocessed.jpg"
        self.mosaic_path = "mosaiced.jpg"
        self.mosiac_object_key = "recognition/image1/mosiac.jpg"
        self.s3_client = boto3.client('s3')
        
    
    def mosaic(self, local):
        if not local:
            try:
                self.s3_client.download_file(self.bucket_name, self.preprocessed_object_key, self.img_download_path)
                print("Image downloaded successfully from S3")
            except Exception as e:
                print(f"Error downloading image from S3: {e}")
        else:
            print("Get img from local")
        
        img = cv2.imread(self.img_download_path, 1)
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
        
        cv2.imwrite(self.mosaic_path, img)
        
        if not local:
            try:
                self.s3_client.upload_file(self.mosaic_path, self.bucket_name, self.mosiac_object_key)
                print("Mosaiced img uploaded successfully to S3")
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
                        success = self.mosaic(local)
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
                    print("An error: ", e)
                    # continue
        except KeyboardInterrupt:
            pass