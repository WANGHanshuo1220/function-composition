import json
import numpy as np
from tensorflow.keras.models import load_model
from tensorflow.keras.preprocessing import image
import os
import cv2 as cv2
from PIL import Image
import pytesseract
import sys
from sys import argv
from googletrans import Translator
import time
import boto3
from wand.image import Image
from botocore.exceptions import ClientError

ENV_WORKDIR="/home/ubuntu/fc/"
bucket_name = "function-composition"

image_path = "porn.jpg"
model_path = "resnet50_final_adult.h5"
json_illegal_path = "is_illegal.json"
json_mdata_path = 'image_metadata.json'
img_download_path = "porn_dl.jpg"
preprocessed_path = "preprocessed.jpg"
model_download_path = "model.h5"
mosaic_path = "mosaiced.jpg"

model_object_key = "recognition/model.h5"
img_object_key = "recognition/image1/image.jpg"
mdata_object_key = "recognition/image1/mdata.json"
preprocessed_object_key = "recognition/image1/preprocessed.jpg"
illegal_object_key = "recognition/image1/illegal.json"
mosiac_object_key = "recognition/image1/mosiac.jpg"

s3_client = boto3.client('s3')
    
try:
    s3_client.upload_file(model_path, bucket_name, model_object_key)
    print("Model uploaded successfully to S3")
except Exception as e:
        print(f"Error uploading Model to S3: {e}")

################################# upload image to s3 #################################
try:
    s3_client.upload_file(image_path, bucket_name, img_object_key)
    print("Image uploaded successfully to S3")
except Exception as e:
        print(f"Error uploading image to S3: {e}")

print("Step 1. finished")

################################# extract&save image metadata #################################
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

print("Step 2. finished")

################################# preprocess image #################################
process = True
try:
    s3_client.head_object(Bucket=bucket_name, Key=mdata_object_key)
except ClientError as e:
    if e.response['Error']['Code'] == '404':
        process = False
    else:
        print("Unexpected error!")
        raise

if process:
    try:
        s3_client.download_file(bucket_name, img_object_key, img_download_path)
        print("Image downloaded successfully from S3")
    except Exception as e:
        print(f"Error downloading image from S3: {e}")

    with Image(filename=img_download_path) as img:
        img.resize(224, 224)
        preprocessed_img = img.clone()
        
        with open(preprocessed_path, 'wb') as f:
            preprocessed_img.save(file=f)
        
    try:
        s3_client.upload_file(preprocessed_path, bucket_name, preprocessed_object_key)
        print("Preprocessed img uploaded successfully to S3")
    except Exception as e:
        print(f"Error uploading Mdata to S3: {e}")

print("Step 3. finished")

# ################################# recognition #################################
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

try:
    s3_client.upload_file(json_illegal_path, bucket_name, illegal_object_key)
    print("Illegal json uploaded successfully to S3")
except Exception as e:
    print(f"Error uploading illegal json to S3: {e}")

print("Step 4. finished")

################################# mosaic&save #################################
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

mosaic_filepath = os.path.join(ENV_WORKDIR, mosaic_path)
cv2.imwrite(mosaic_filepath, img)

try:
    s3_client.upload_file(mosaic_path, bucket_name, mosiac_object_key)
    print("Preprocessed img uploaded successfully to S3")
except Exception as e:
    print(f"Error uploading Mdata to S3: {e}")

print("Step 5. finished")
