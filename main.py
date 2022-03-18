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

@app.get("/get_redis/{redis_key}")
def root(redis_key: str):
    out = red.get(redis_key)
    if out == None or out == "":
        return "Not found"
    return red.get(redis_key)

@app.get("/set_redis/{rediskey}/{redisvalue}")
def root(rediskey: str,redisvalue:str):
    red.set(rediskey,redisvalue)
    return red.get(rediskey)



@app.get("/")
async def root():
    return {"REST Server": "Image Recognition API Server"}




@app.post("/upload")
def upload_file(myfile: UploadFile = File(...)):
    start_time = time.time()
    new_dir_path = os.getcwd() + "/images/"

    destination_file_path = new_dir_path+myfile.filename #output file path

    # file_location = f"files/{uploaded_file.filename}"
    with open(destination_file_path, "wb+") as file_object:
        file_object.write(myfile.file.read())
    b = push_images_to_sqs(myfile.filename)
    # name = myfile.filename
    # name = name.strip('.jpg')
    # a = push_to_response_queue(myfile.filename,correct_map.get(name,''))
    # time.sleep(1)
    # a =  receive_messages_from_sqs()
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


@app.get("/dev/delete_from_queue/")
async def delete_sqs():
    while receive_msg_and_delete_image() != False:
        continue
    return "Yes"

@app.get("/dev/flush_redis/")
async def flush_redis():
    red.flushdb()
    return True


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
    print("running")
