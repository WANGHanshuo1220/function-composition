import sys
from imageai.Detection import ObjectDetection
from multiprocessing import Process, Manager
import time
import boto3
from boto3.s3.transfer import TransferConfig
from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageFile
import zipfile
import os
import json
import subprocess

ImageFile.LOAD_TRUNCATED_IMAGES = True

class classify:
    def __init__(self, video_type, worker_id) -> None:
        self.s3_client = boto3.client('s3')
        self.s3_bucket_name = "function-composition"
        self.s3_frame_key = "video/" + video_type + "/frames/"
        self.s3_mdata_key = "video/" + video_type + "/mdata.json"
        self.s3_model_key = "video/yolo-tiny.h5"
        self.s3_detection_key = "video/" + video_type + "/frame_detection/"
    
        self.local_mdata_path = "/tmp/mdata.json"
        self.local_model_path = "/tmp/model.h5"
        self.local_worker_dir = "/tmp/" + "worker_" + str(self.worker_id)

        self.video_type = video_type
        self.worker_id = worker_id

        self.clean_all()


    def clean_all(self):
        self.clean_s3()
        self.clean_local()

        
    def clean_s3(self):
        try:
            bucket = boto3.resource('s3').Bucket(self.s3_bucket_name)
            for obj in bucket.objects.filter(Prefix = self.s3_detection_key):
                obj.delete()
        except:
            pass


    def clean_local(self):
        try:
            with open('/dev/null', 'w') as devnull:
                subprocess.call('rm /tmp/frame_* /tmp/mdata.json', 
                                shell = True,
                                stdout = devnull,
                                stderr = devnull)
        except:
            pass


    def delete_tmp(self):
        for root, dirs, files in os.walk("/tmp/", topdown=False):
           for name in files:
              os.remove(os.path.join(root, name))
           for name in dirs:
              os.rmdir(os.path.join(root, name))

 
    def detect_object(self, frame_num, detect_prob):
        if not os.path.exists(self.local_worker_dir):
            os.mkdir(self.local_worker_dir)

        frame_base_path = self.local_worker_dir + "/frame_" + str(frame_num)
    
        input_frame = frame_base_path + "/org_frame_" + str(frame_num) + ".jpg"
        config = TransferConfig(use_threads = True)
        frame_key = self.s3_frame_key + "frame_" + str(frame_num) + ".jpg"

        f = open(input_frame, "wb")
        self.s3_client.download_fileobj(self.s3_bucket_name, 
                                        frame_key, f, 
                                        Config = config)
        f.close()
 
        f = open(self.local_model_path, "wb")
        self.s3_client.download_fileobj(self.s3_bucket_name, 
                                        self.s3_model_key, 
                                        f, Config = config)
        f.close()

        detector = ObjectDetection()
        detector.setModelTypeAsTinyYOLOv3()
        detector.setModelPath(self.local_model_path)
        detector.loadModel()
    
        output_path = frame_base_path + "/detection_" + str(frame_num) + ".jpg"

        detection = detector.detectObjectsFromImage(
            input_image = input_frame, 
            output_image_path = output_path, 
            minimum_percentage_probability = detect_prob)
         
        for box in range(len(detection)):
            print(detection[box])
         
        if len(detection) > 10 :
            original_image = Image.open(input_frame, mode = 'r')
            ths = []
            threads = 4
            start_index = 0
            step_size = int(len(detection) / threads) + 1
           
            for t in range(threads):
                end_index = min(start_index + step_size , len(detection))
                ths.append(Process(target = self.crop_and_sharpen, 
                                   args=(original_image.copy(), 
                                   t, detection, start_index,
                                   end_index, frame_base_path)))
                start_index = end_index

            for t in range(threads):
                ths[t].start()

            for t in range(threads):
                ths[t].join()


        # millis_3 = int(round(time.time() * 1000))
        zipFileName = "detected_frame_" + str(frame_num) + ".zip"
        myzip = zipfile.ZipFile("/tmp/" + zipFileName, 
                                'w', zipfile.ZIP_DEFLATED)
        
        for f in os.listdir(frame_base_path):
            myzip.write(frame_base_path + "/" + f) 
        
        self.s3_client.upload_file("/tmp/" + zipFileName, 
                                   self.s3_bucket_name, 
                                   self.s3_detection_key + zipFileName, 
                                   Config = config)
        print("file uploaded " + zipFileName) 	

    
    def crop_and_sharpen(self, original_image, t, detection ,start_index, end_index, worker_dir):
        for box in range(start_index, end_index):
                im_temp = original_image.crop((detection[box]['box_points'][0], 
                                               detection[box]['box_points'][1], 
                                               detection[box]['box_points'][2], 
                                               detection[box]['box_points'][3]))
                im_resized = im_temp.resize((1408, 1408))
                im_resized_sharpened =  im_resized.filter(ImageFilter.SHARPEN)
                fileName = worker_dir + "/" + detection[box]['name'] + \
                           "_" + str(box) + \
                           "_" + str(t) + \
                           "_" + ".jpg"
                im_resized_sharpened.save(fileName)
    
        # print(len(detection))
    

    def classify(self):
        start_time = int(round(time.time() * 1000))
        
        # Download json file from s3 
        self.s3_client.download_file(self.s3_bucket_name, 
                                     self.s3_mdata_key,
                                     self.local_mdata_path)
    
        # Extract json file
        with open(self.local_mdata_path, 'r') as file:
            json_data = json.load(file)
        frame_num = json_data["indeces"][self.worker_id]['values']
        detect_prob = json_data["indeces"][self.worker_id]['detect_prob']
        print(frame_num)
    
        ths = []
        num_threads = len(frame_num)
        for w in range(num_threads):
        
            ths.append(Process(target = self.detect_object, 
                               args = (frame_num[w], detect_prob)))
    
        for t in range(num_threads):
            ths[t].start()
        for t in range(num_threads):
            ths[t].join()
    
        end_time = time.time() * 1000
        diff_time = str( end_time  - start_time )
        print("duration: " + diff_time )
        # return { 'duration': diff_time, 'values': str(event['values']) }
    

if __name__ == "__main__":
    c = classify("small", 6)
    c.classify()