# main.py

from fastapi import FastAPI, UploadFile, File
from typing import List
import os
import aiofiles

import boto3 

from utils import *

app = FastAPI()
# queue_url="https://sqs.us-east-1.amazonaws.com/027200419369/Queue1.fifo"
# queue_url = "https://sqs.us-east-1.amazonaws.com/247558419887/sqs-send.fifo"
# sqs_client = boto3.client('sqs', region_name='us-east-1')


@app.get("/")
async def root():
    return {"message": "Hello World"}

# @app.post("/upload")
# async def upload_file(files: List[UploadFile] = File(...)):
#     new_dir_path = os.getcwd() + "/images/"
#     if not os.path.exists(new_dir_path):
#         os.makedirs(new_dir_path)
#     a = []
#     b = ""
#     for file in files:
#         destination_file_path = new_dir_path+file.filename #output file path
#         async with aiofiles.open(destination_file_path, 'wb') as out_file:
#             while content := await file.read(1024):  # async read file chunk
#                 await out_file.write(content)  # async write file chunk
#         if b == "":
#             b = file.filename
#         else:
#             b = b+","+file.filename
#     print(push_images_to_sqs(b))
#     return {"Result": "OK", "filenames": [file.filename for file in files]}

@app.post("/upload")
async def upload_file(myfile: UploadFile = File(...)):
    new_dir_path = os.getcwd() + "/images/"
    if not os.path.exists(new_dir_path):
        os.makedirs(new_dir_path)

    destination_file_path = new_dir_path+myfile.filename #output file path
    async with aiofiles.open(destination_file_path, 'wb') as out_file:
        content = await myfile.read()
        await out_file.write(content)
        # while content := await myfile.read(1024):  # async read file chunk
        #     await out_file.write(content)  # async write file chunk

    a = push_images_to_sqs(myfile.filename)
    return a+": ",myfile.filename

@app.get("/delete")
async def delete_sqs():
    while receive_msg_and_delete_image() != False:
        continue
    return "Yes"
    # return {"Result": "OK", "filenames": [file.filename for file in files]}
    # for file in files:
    #     with open(file.filename, 'wb') as image:
    #         content = await file.read()
    #         image.write(content)
    #         image.close()
    #     a.append(file.filename)
    # return a
    # return JSONResponse(content={"filenames": file.filename})


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
    print("running")
