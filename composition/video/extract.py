#!/usr/bin/env python
import os
from urllib.parse import unquote_plus
import json
import boto3
from boto3.s3.transfer import TransferConfig
import subprocess
import re
import time
import json

import grpc
import message_pb2 as pb2
import message_pb2_grpc as pb2_grpc

FFMPEG_STATIC = "var/ffmpeg"


class extract:
    def __init__(self, video_type, worker_id) -> None:
        self.step = 2

        self.s3_client = boto3.client('s3')
        self.s3_bucket_name = "function-composition"
        self.s3_chunk_key = "video/" + video_type + "/chunks/"
        self.s3_frame_key = "video/" + video_type + "/frames/"
        self.s3_mdata_key = "video/" + video_type + "/mdata.json"

        self.local_mdata_path = "/tmp/mdata.json"
        self.video_type = video_type
        self.worker_id = worker_id
        
        self.clean_all()


    def clean_all(self):
        self.clean_s3()
        self.clean_local()

        
    def clean_s3(self):
        try:
            try:
                response = self.s3_client.list_objects_v2(Bucket = self.s3_bucket_name,
                                                                Prefix = self.s3_frame_key)
            except Exception as e:
                print("Delete error with", e)

            files_in_folder = response["Contents"]
            files_to_delete = []
            # We will create Key array to pass to delete_objects function
            for f in files_in_folder:
                files_to_delete.append({"Key": f["Key"]})

            # This will delete all files in a folder
            response = self.s3_client.delete_objects(
                Bucket = self.s3_bucket_name, Delete={"Objects": files_to_delete}
            )
        except:
            pass


    def clean_local(self):
        try:
            with open('/dev/null', 'w') as devnull:
                subprocess.call('rm /tmp/*.mp4 /tmp/frame_* /tmp/mdata.json', 
                                shell = True,
                                stdout = devnull,
                                stderr = devnull)
        except:
            pass
 
    
    def extract(self):
        config = TransferConfig(use_threads=True)

        # Download json file from s3 
        self.s3_client.download_file(self.s3_bucket_name, 
                                     self.s3_mdata_key,
                                     self.local_mdata_path)
    
        # Extract json file
        with open(self.local_mdata_path, 'r') as file:
            json_data = json.load(file)
        chunks_nums = json_data["indeces"][self.worker_id]['values']
        
        # For every chunk in the chunk_list, extract frame
        count = 0
        extract_millis = []
        for record in chunks_nums:
            count = count + 1

            filename = "/tmp/chunk_" + str(record) + ".mp4"
            video_name = "chunk_" + str(record) + ".mp4"

            f = open(filename, "wb")
            self.s3_client.download_fileobj(self.s3_bucket_name, 
                                            self.s3_chunk_key + video_name,
                                            f, Config = config)
            f.close()

            millis = int(round(time.time() * 1000))
            extract_millis.append(millis)

            frame_name = video_name.replace("chunk", "frame") \
                                   .replace("mp4", "jpg")

            # Actual extract process
            with open('/dev/null', 'w') as devnull:
                try:
                    subprocess.call(["ffmpeg", 
                                     '-i', filename, 
                                     '-frames:v', "1" , 
                                     "-q:v","1", '/tmp/' + frame_name],
                                     stdout = devnull,
                                     stderr = devnull)
                except:
                    print("Extract subprocess error")

            try:
                self.s3_client.upload_file("/tmp/" + frame_name, 
                                           self.s3_bucket_name, 
                                           self.s3_frame_key + frame_name, 
                                           Config=config)
            except:
                print("Upload video frame after extract failure!")
                
        print("Done!") 

        return True


    def run(self):
        host_ip = os.environ.get("FC_HOST_IP")
        channel = grpc.insecure_channel(str(host_ip) + ':50051')
        stub = pb2_grpc.NodeCommStub(channel)

        try:
            while True:
                try:
                    response = stub.FC_NodeComm(pb2.RequestInfo(step = self.step, 
                                                                finished = False))
                    if response.process:
                        print("Received message from server:", response.process)
                        local = response.local

                        # do the job
                        start_time = time.time()
                        success = self.extract()
                        end_time = time.time()

                        # send job finished to master
                        res = stub.FC_NodeComm(pb2.RequestInfo(
                            step = self.step, 
                            finished = success, 
                            exec_time = end_time-start_time
                            ))

                        if res.exit:
                            self.clean_local()
                            return

                except Exception as e:
                    # print("An error occured: ", e)
                    continue

        except KeyboardInterrupt:
            exit(0)

