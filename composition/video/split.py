#!/usr/bin/env python
import os
import json
import boto3
from boto3.s3.transfer import TransferConfig
import subprocess
import re
import time
import signal
import math

import grpc
import message_pb2 as pb2
import message_pb2_grpc as pb2_grpc

FFMPEG_STATIC = "var/ffmpeg"

length_regexp = 'Duration: (\d{2}):(\d{2}):(\d{2})\.\d+,'
re_length = re.compile(length_regexp)

class split:
    def __init__(self, video_type, parallel, s3_client) -> None:
        self.step = 1

        # self.s3_client = boto3.client('s3')
        self.s3_client =  s3_client
        self.s3_bucket_name = "function-composition"
        self.s3_source_key = "video/"+ video_type + "/video.mp4"
        self.s3_chunk_key = "video/"+ video_type + "/chunks/"
        self.s3_mdata_key = "video/"+ video_type + "/mdata.json"

        self.local_video_path = "/tmp/src.mp4"
        self.local_json_path = "/tmp/mdata.json"
        self.video_type = video_type
        self.parallel = parallel

        self.s3_UpOrDownload_time = 0.0

        # self.Clean_all()
        self.Clean_local()

    
    def Clean_all(self):
        self.Clean_s3()
        self.Clean_local()

        
    def Clean_s3(self):
        try:
            self.s3_client.delete_object(Bucket = self.s3_bucket_name, 
                                         Key    = self.s3_mdata_key)

            try:
                response = self.s3_client.list_objects_v2(Bucket = self.s3_bucket_name,
                                                                Prefix = self.s3_chunk_key)
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


    def Clean_local(self):
        try:
            with open('/dev/null', 'w') as devnull:
                subprocess.call('rm /tmp/*.mp4 /tmp/mdata.json', 
                                shell = True,
                                stdout = devnull,
                                stderr = devnull)
        except:
            pass

        
    def Get_s3_time(self):
        return self.s3_UpOrDownload_time
 

    def Split(self):
        config = TransferConfig(use_threads=False)

        with open(self.local_video_path, "wb") as f:
            start_time = time.time()
            self.s3_client.download_fileobj(self.s3_bucket_name, self.s3_source_key, 
                                            f, Config=config)
            end_time = time.time()
            self.s3_UpOrDownload_time += (end_time - start_time)

        with subprocess.Popen("ffmpeg" + " -i '" + self.local_video_path + "' 2>&1 | grep 'Duration'",
                              shell = True,
                              stdout = subprocess.PIPE) as process:
            output, _ = process.communicate()
            output = output.decode("utf-8")
            matches = re_length.search(output)
        process.kill()

        # Number of splits
        count = 0
        millis_list = []

        if matches:
            video_length = int(matches.group(1)) * 3600 + \
                           int(matches.group(2)) * 60 + \
                           int(matches.group(3))
            # print("Video length in seconds: " + str(video_length))

            start = 0
            chunk_size = 2 # in seconds
            while (start < video_length):
                # end = min(video_length - start,chunk_size)
                end = min(start + chunk_size, video_length)
                millis = int(round(time.time() * 1000))
                millis_list.append(millis)
                chunk_video_name = "chunk_" + str(count) + '.mp4'
                with open('/dev/null', 'w') as devnull:
                    try:
                        subprocess.call(["ffmpeg", 
                                        '-i', self.local_video_path, 
                                        '-ss', str(start) , 
                                        '-t', str(end),
                                        '-c', 'copy', '/tmp/' + chunk_video_name],
                                        stdout = devnull,
                                        stderr = devnull)
                    except Exception as e:
                        print(f"Split subprocess error {e}")

                count = count + 1
                start = start + chunk_size
                start_time = time.time()
                try:
                    self.s3_client.upload_file("/tmp/" + chunk_video_name, 
                                               self.s3_bucket_name, 
                                               self.s3_chunk_key + chunk_video_name, 
                                               Config = config)
                    # print(f"Split upload {chunk_video_name} success")
                except Exception as e:
                    print(f"Split upload {chunk_video_name} error with {e}")
                    break
                end_time = time.time()
                self.s3_UpOrDownload_time += (end_time - start_time)

        # print("Done!") 

        # Number of video chunks per downstream worker
        payload = int(count / self.parallel)
        listOfDics = []   
        currentList = []
        currentMillis = []
        for i in range(count):
            if len(currentList) < payload:
               currentList.append(i)
               currentMillis.append(millis_list[i]) 
            if len(currentList) == payload or i == count-1:
               tempDic = {}
               tempDic['values'] = currentList
            #    tempDic['source_id'] = self.video_type
            #    tempDic['millis'] = currentMillis
               tempDic['detect_prob'] = 2
               listOfDics.append(tempDic)
               currentList = []
               currentMillis = []

        vedio_split_info = {
            "indeces": listOfDics 
        }
        # print(f"mdata.json: {vedio_split_info}")

        with open(self.local_json_path, 'w') as json_file:
            json.dump(vedio_split_info, json_file, indent = 4)

        start_time = time.time()
        self.s3_client.upload_file(self.local_json_path, 
                                   self.s3_bucket_name, 
                                   self.s3_mdata_key)
        end_time = time.time()
        self.s3_UpOrDownload_time += (end_time - start_time)

        # print("Upload mdata.json done")

        return True
    
    
def Run_split(worker_id, parallel, s3_client, return_dict):
    # print(f"Exec split at time {time.time()}")
    s = split("small", parallel, s3_client)

    start_time = time.time()
    s.Split()
    end_time = time.time()
    total_exec_time = end_time - start_time

    return_dict.append(total_exec_time-s.Get_s3_time())
    return_dict.append(s.Get_s3_time())
    
    # print(f"Finished split at time {time.time()}")