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


class BackgroundRunner:
    def __init__(self):
        self.value = 0
        self.dict = {}

    def receive_messages_from_sqs(self):
        sqs_client = boto3.client('sqs', region_name='us-east-1')
        try:
            response = sqs_client.receive_message(QueueUrl=response_queue_url,MaxNumberOfMessages=1, MessageAttributeNames=['All'])
        except Exception as e:
            print('Could not receive the message from the Queue!!!', e)
            raise
        else:
            # print("response : ",response)
            if len(response.get("Messages", [])) > 0:
                self.value += 1
                json_data = json.loads(response.get("Messages", [])[0]["Body"], strict = False)
                # img_data = json_data["encoded_img_data"]
                img_name = response.get("Messages", [])[0]["MessageAttributes"]["image_name"]["StringValue"]
                # print(json_data['img_name'],json_data['img_output'])

                # response format for referecne
                # [{'MessageId': '9843b8c4-f565-44fc-9b18-5b06fda6421a', 'ReceiptHandle': 'AQEBywr1zXAnD3lgeuAHfl2CRAMfJKK+B/WUa4rlEL7tBcZk5GVOVjlDEJa+Dtjw5tqZxh9V5VaJSj03xSjyEwROnBG4b45WN6KxOqlEXBRXFJCiCYclSTKB+zz5iAjO47jTryVWh2/L/+FB8MHXRYnWFn/037d4SGzr7coKPFvYKN7Vqd4Gk3Uuixm4lzQG3iKZsItPpnUmwH1lMVqmrqza1liV+aWq91VhRPfKwyRaoxNCOmvgBafVPpvI339KAJ5gXeUiKKTxuqjlTF9YFA3lc0W4SfTDV0h1SnsT7je0AeicL2+iMRo2KNaFAzTzcWz3sftp8LDpCk1Vh5IplWnX1MOPIUzXnqm+wh5qqVWJGIZ9UC6FPt8gAvUCtkUtcgZXUPWQI3qssflxsTLCydu9oQ==', 'MD5OfBody': '5d138413a2203342b327a4399c0954f4', 'Body': '{ "img_name" : "test_29.jpg" , "img_output" : "Wang" }', 'MD5OfMessageAttributes': '00ce38a3679f52b4c177713618dacf28', 'MessageAttributes': {'image_name': {'StringValue': 'test_29.jpg', 'DataType': 'String'}}}]



                message_receipt_handle = response.get("Messages", [])[0]["ReceiptHandle"]
                if len(message_receipt_handle) > 0:
                        print("Deleting message with image name : ",img_name," ...")
                        delete_response = sqs_client.delete_message(QueueUrl=response_queue_url,ReceiptHandle=message_receipt_handle)
                        # print("Delete response : ",delete_response)
                
                filename = str(json_data['img_name']).replace('.jpg', '').strip()
                output_val = str(json_data['img_output'])
                output_val = output_val.strip("\n")
                self.dict[filename] = output_val

                # crct = str(correct_map.get(filename,''))
                # out = (str(filename),crct)
                # out = '('+str(filename)+','+str(crct)+')'
                # return out
            else:
                print(" --- No new messages to read from the queue.")

    async def run_main(self):
        while True:
            await asyncio.sleep(2)
            self.receive_messages_from_sqs()
            

runner = BackgroundRunner()

@app.on_event('startup')
async def app_startup():
    asyncio.create_task(runner.run_main())

@app.get("/runner_value")
def root():
    return runner.dict

@app.get("/output_count")
def root():
    return runner.value



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
        print("\n\n",name,", count ",count, ":  dict - ", runner.dict,"\n\n\n")
        if name in runner.dict.keys():
            result = runner.dict[name]
            print(name,result)
            del runner.dict[name]
            return str(result)
            # return (name,result)
            break
        else:
            time.sleep(3)


@app.get("/dev/delete_from_queue/")
async def delete_sqs():
    while receive_msg_and_delete_image() != False:
        continue
    return "Yes"


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
    print("running")
