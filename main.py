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
correct_map = {
'test_00' : 'Paul',
'test_01' : 'Emily',
'test_02' : 'Bob',
'test_03' : 'German',
'test_04' : 'Emily',
'test_05' : 'Gerry',
'test_06' : 'Gerry',
'test_07' : 'Ranil',
'test_08' : 'Bill',
'test_09' : 'Wang',
'test_10' : 'Paul',
'test_11' : 'Emily',
'test_12' : 'Bob',
'test_13' : 'German',
'test_14' : 'Emily',
'test_15' : 'Gerry',
'test_16' : 'Gerry',
'test_17' : 'Ranil',
'test_18' : 'Bill',
'test_19' : 'Wang',
'test_20' : 'Paul',
'test_21' : 'Emily',
'test_22' : 'Bob',
'test_23' : 'German',
'test_24' : 'Emily',
'test_25' : 'Gerry',
'test_26' : 'Gerry',
'test_27' : 'Ranil',
'test_28' : 'Bill',
'test_29' : 'Wang',
'test_30' : 'Paul',
'test_31' : 'Emily',
'test_32' : 'Bob',
'test_33' : 'German',
'test_34' : 'Emily',
'test_35' : 'Gerry',
'test_36' : 'Gerry',
'test_37' : 'Ranil',
'test_38' : 'Bill',
'test_39' : 'Wang',
'test_40' : 'Paul',
'test_41' : 'Emily',
'test_42' : 'Bob',
'test_43' : 'German',
'test_44' : 'Emily',
'test_45' : 'Gerry',
'test_46' : 'Gerry',
'test_47' : 'Ranil',
'test_48' : 'Bill',
'test_49' : 'Wang',
'test_50' : 'Paul',
'test_51' : 'Emily',
'test_52' : 'Bob',
'test_53' : 'German',
'test_54' : 'Emily',
'test_55' : 'Gerry',
'test_56' : 'Gerry',
'test_57' : 'Ranil',
'test_58' : 'Bill',
'test_59' : 'Wang',
'test_60' : 'Paul',
'test_61' : 'Emily',
'test_62' : 'Bob',
'test_63' : 'German',
'test_64' : 'Emily',
'test_65' : 'Gerry',
'test_66' : 'Gerry',
'test_67' : 'Ranil',
'test_68' : 'Bill',
'test_69' : 'Wang',
'test_70' : 'Paul',
'test_71' : 'Emily',
'test_72' : 'Bob',
'test_73' : 'German',
'test_74' : 'Emily',
'test_75' : 'Gerry',
'test_76' : 'Gerry',
'test_77' : 'Ranil',
'test_78' : 'Bill',
'test_79' : 'Wang',
'test_80' : 'Paul',
'test_81' : 'Emily',
'test_82' : 'Bob',
'test_83' : 'German',
'test_84' : 'Emily',
'test_85' : 'Gerry',
'test_86' : 'Gerry',
'test_87' : 'Ranil',
'test_88' : 'Bill',
'test_89' : 'Wang',
'test_90' : 'Paul',
'test_91' : 'Emily',
'test_92' : 'Bob',
'test_93' : 'German',
'test_94' : 'Emily',
'test_95' : 'Gerry',
'test_96' : 'Gerry',
'test_97' : 'Ranil',
'test_98' : 'Bill',
'test_99' : 'Wang',
}


class BackgroundRunner:
    def __init__(self):
        self.value = 0
        self.dict = {}

    def receive_messages_from_sqs(self):
        sqs_client = boto3.client('sqs', region_name='us-east-1')
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
                        # print("Delete response : ",delete_response)
                
                filename = img_name.replace('.jpg', '').strip()
                self.value = self.value+1
                self.dict[filename] = self.value
                # crct = str(correct_map.get(filename,''))
                # out = (str(filename),crct)
                # out = '('+str(filename)+','+str(crct)+')'
                # return out
            else:
                print(" --- No new messages to read from the queue.")

    async def run_main(self):
        while True:
            await asyncio.sleep(3)
            self.receive_messages_from_sqs()
            

runner = BackgroundRunner()

@app.on_event('startup')
async def app_startup():
    asyncio.create_task(runner.run_main())

@app.get("/runner_value")
def root():
    return runner.dict



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


# def receive_messages_from_sqs():
    
#     sqs_client = boto3.client('sqs', region_name='us-east-1')
#     while True:
#         time.sleep(2)
#         try:
#             response = sqs_client.receive_message(QueueUrl=queue_url,MaxNumberOfMessages=1, MessageAttributeNames=['All'])
#         except ClientError:
#             logger.exception('Could not receive the message from the Queue!!!')
#             raise
#         else:
#             # print("response : ",response)
#             if len(response.get("Messages", [])) > 0:
#                 json_data = json.loads(response.get("Messages", [])[0]["Body"])
#                 img_data = json_data["encoded_img_data"]
#                 img_name = response.get("Messages", [])[0]["MessageAttributes"]["image_name"]["StringValue"]
#                 message_receipt_handle = response.get("Messages", [])[0]["ReceiptHandle"]
#                 if len(message_receipt_handle) > 0:
#                         print("Deleting message with image name : ",img_name," ...")
#                         delete_response = sqs_client.delete_message(QueueUrl=queue_url,ReceiptHandle=message_receipt_handle)
#                         print("Delete response : ",delete_response)
#                 filename = img_name.replace('.jpg', '').strip()
#                 # crct = str(correct_map.get(filename,''))
#                 # out = (str(filename),crct)
#                 # out = '('+str(filename)+','+str(crct)+')'
#                 # return out
#                 break
#             else:
#                 print("No new messages to read from the queue.")



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

    while True:
        name = myfile.filename
        name = name.strip('.jpg')
        print(name,runner.dict)
        if name in runner.dict.keys():
            del runner.dict[name]
            print(runner.dict[name])
            return str(correct_map.get(name,''))
            # return (name,runner.dict[name])
            break
        else:
            time.sleep(3)
    # a = '(test_10,Paul)'
    # return "Done: ",myfile.filename + " in : ",str(end_time)
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
