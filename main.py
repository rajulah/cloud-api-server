# main.py

from fastapi import FastAPI, UploadFile, File
from typing import List
import os
# import aiofiles

import boto3 

from utils import *
import time
app = FastAPI()


@app.get("/")
async def root():
    return {"REST Server": "Image Recognition API Server"}

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


def receive_messages_from_sqs():
    
    sqs_client = boto3.client('sqs', region_name='us-east-1')
    while True:
        time.sleep(2)
        try:
            response = sqs_client.receive_message(QueueUrl=queue_url,MaxNumberOfMessages=1, MessageAttributeNames=['All'])
        except ClientError:
            logger.exception('Could not receive the message from the Queue!!!')
            raise
        else:
            # print("response : ",response)
            if len(response.get("Messages", [])) > 0:
                json_data = json.loads(response.get("Messages", [])[0]["Body"])
                img_data = json_data["encoded_img_data"]
                img_name = response.get("Messages", [])[0]["MessageAttributes"]["image_name"]["StringValue"]
                message_receipt_handle = response.get("Messages", [])[0]["ReceiptHandle"]
                if len(message_receipt_handle) > 0:
                        print("Deleting message with image name : ",img_name," ...")
                        delete_response = sqs_client.delete_message(QueueUrl=queue_url,ReceiptHandle=message_receipt_handle)
                        print("Delete response : ",delete_response)
                filename = img_name.replace('.jpg', '').strip()
                # crct = str(correct_map.get(filename,''))
                # out = (str(filename),crct)
                # out = '('+str(filename)+','+str(crct)+')'
                # return out
                break
            else:
                print("No new messages to read from the queue.")



@app.post("/upload")
def upload_file(myfile: UploadFile = File(...)):
    start_time = time.time()
    new_dir_path = os.getcwd() + "/images/"

    destination_file_path = new_dir_path+myfile.filename #output file path

    # file_location = f"files/{uploaded_file.filename}"
    with open(destination_file_path, "wb+") as file_object:
        file_object.write(myfile.file.read())
    b = push_images_to_sqs(myfile.filename)
    # time.sleep(1)
    # a =  receive_messages_from_sqs()
    end_time = (time.time()-start_time)
    # print("Done: ",myfile.filename + " in : ",str(end_time))

    # a = '(test_10,Paul)'
    return "Done: ",myfile.filename + " in : ",str(end_time)
    # return a.strip('"')
    # return str(a) +" in : "+ str(end_time) + "   ",str(b)


@app.get("/dev/delete_from_queue/")
async def delete_sqs():
    while receive_msg_and_delete_image() != False:
        continue
    return "Yes"


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
    print("running")
