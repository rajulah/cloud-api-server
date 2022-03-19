# main.py

from fastapi import FastAPI, UploadFile, File
from typing import List
import os
# import aiofiles
import asyncio
import boto3 

from utils import *
import time
app = FastAPI()

import redis
red = redis.Redis(
host= 'localhost',
port= '6379')

red.set('mykey', 'Hello from Python!')
value = red.get('mykey') 


# API used only by developer for testing during development
# @app.get("/get_redis/{redis_key}")
# def root(redis_key: str):
#     out = red.get(redis_key)
#     if out == None or out == "":
#         return "Not found"
#     return red.get(redis_key)

# API used only by developer for testing during development
# @app.get("/set_redis/{rediskey}/{redisvalue}")
# def root(rediskey: str,redisvalue:str):
#     red.set(rediskey,redisvalue)
#     return red.get(rediskey)



@app.get("/")
async def root():
    return {"REST Server": "Image Recognition API Server"}




@app.post("/upload")
def upload_file(myfile: UploadFile = File(...)):
    start_time = time.time()
    new_dir_path = os.getcwd() + "/images/"

    destination_file_path = new_dir_path+myfile.filename #output file path

    with open(destination_file_path, "wb+") as file_object:
        file_object.write(myfile.file.read())
    b = push_images_to_sqs(myfile.filename)

    end_time = (time.time()-start_time)
    # print("Done: ",myfile.filename + " in : ",str(end_time))
    count = 0
    while True:
        count += 1
        name = myfile.filename
        name = name.strip('.jpg')
        res = red.get(name)
        if res!= None and res!="":
            res = res.decode("utf-8") 
            print(name,res)
            red.delete(name)
            return str(res)
            # return (name,result)
            break
        else:
            time.sleep(3)


# API used only by developer for testing during development
# @app.get("/dev/delete_from_request_queue/")
# async def delete_request_sqs():
#     while receive_msg_and_delete_image() != False:
#         continue
#     return "Yes"

# API used only by developer for testing during development
# @app.get("/dev/delete_from_response_queue/")
# async def delete_response_sqs():
#     while delete_from_response_queue() != False:
#         continue
#     return "Yes"

# API used only by developer for testing during development
# @app.get("/dev/flush_redis/")
# async def flush_redis():
#     red.flushdb()
#     return True


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=3000)
    print("running")
