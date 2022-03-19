import base64
import boto3
import os 
import json

def get_encoded_img_data(image_name):
    # print("Encoding image : ",image_name," ...")
    image = open(image_name, 'rb')
    image_read = image.read()
    image_64_encode = base64.b64encode(image_read)
#    print("---------------")
#    print(image_64_encode)
#    print("---------------")
    return image_64_encode

image_source_folder = "images/"
image_destination_path = ""

# make sure the aws credentials are configured in .aws folder for boto3 to access


# standard queues

# make sure the aws credentials are configured in .aws folder for boto3 to access

#harish sqs urls (comment and uncomment according to who is using the app)
queue_url = "https://sqs.us-east-1.amazonaws.com/247558419887/cc-project-request-queue"
response_queue_url = "https://sqs.us-east-1.amazonaws.com/247558419887/cc-project-response-queue"

#suraj sqs urls (comment and uncomment according to who is using the app)
# queue_url = "https://sqs.us-east-1.amazonaws.com/027200419369/ImageQueueStandard"
# response_queue_url = "https://sqs.us-east-1.amazonaws.com/027200419369/resultQueueStandard"

sqs_client = boto3.client('sqs', region_name='us-east-1')

def push_images_to_sqs(images_list_string):
    # images_list_string = sys.argv[1]
    image_list = images_list_string.split(",")
    sqs_client = boto3.client('sqs', region_name='us-east-1')
    for i in range(len(image_list)):
        if len(image_list[i]) > 0:
            # print("--------------------------------------------------------------------------------")
            # print("pushing image data of : ",image_list[i]," to SQS queue...")
            full_image_path = image_source_folder+image_list[i]
            encoded_image_data = get_encoded_img_data(full_image_path).decode('utf-8')
            json_str = '{ "img_name" : "'+image_list[i]+'" , "encoded_img_data" : "'+encoded_image_data+'" }'
            # print("Image data : ",json_str)
            try:
                response = sqs_client.send_message(QueueUrl=queue_url, MessageBody=json_str, MessageAttributes={ 'image_name': { 'StringValue': image_list[i] , 'DataType': 'String' } } )
            except:
                print("******** Failed  - ",image_list[i])
            else:
                print("SUCC : ",image_list[i],"")
                # print("Response : ",response)
                # print("\n")
                # print("Deleting image file : ",image_list[i]," from source folder....")
                if os.path.exists(full_image_path):
                    os.remove(full_image_path)
                #     print("Delete successful.")
                # else:
                #     print("The image file : ",full_image_path," does not exist !!!") 
            print("--------------------------------------------------------------------------------")

    return "hey"



# function written for testing purposes
# def receive_msg_and_delete_image():
    
#     sqs_client = boto3.client('sqs', region_name='us-east-1')
#     try:
#         response = sqs_client.receive_message(QueueUrl=queue_url,MaxNumberOfMessages=1, MessageAttributeNames=['All'])
#     except ClientError:
#         logger.exception('Could not receive the message from the Queue!!!')
#         raise
#     else:
#         # print("response : ",response)
#         if len(response.get("Messages", [])) > 0:
#             json_data = json.loads(response.get("Messages", [])[0]["Body"])
#             img_data = json_data["encoded_img_data"]
#             img_name = response.get("Messages", [])[0]["MessageAttributes"]["image_name"]["StringValue"]
#             message_receipt_handle = response.get("Messages", [])[0]["ReceiptHandle"]
#             if len(message_receipt_handle) > 0:
#                     print("Deleting message with image name : ",img_name," ...")
#                     delete_response = sqs_client.delete_message(QueueUrl=queue_url,ReceiptHandle=message_receipt_handle)
#                     print("Delete response : ",delete_response)
#         else:
#             print("No new messages to read from the queue.")
#             return False


# function written for testing purposes
# def delete_from_response_queue():
    
#     sqs_client = boto3.client('sqs', region_name='us-east-1')
#     try:
#         response = sqs_client.receive_message(QueueUrl=response_queue_url,MaxNumberOfMessages=1, MessageAttributeNames=['All'])
#     except ClientError:
#         logger.exception('Could not receive the message from the Queue!!!')
#         raise
#     else:
#         # print("response : ",response)
#         if len(response.get("Messages", [])) > 0:
#             # json_data = json.loads(response.get("Messages", [])[0]["Body"])
#             # img_data = json_data["encoded_img_data"]
#             img_name = response.get("Messages", [])[0]["MessageAttributes"]["image_name"]["StringValue"]
#             message_receipt_handle = response.get("Messages", [])[0]["ReceiptHandle"]
#             if len(message_receipt_handle) > 0:
#                     print("Deleting message with image name : ",img_name," ...")
#                     delete_response = sqs_client.delete_message(QueueUrl=queue_url,ReceiptHandle=message_receipt_handle)
#                     print("Delete response : ",delete_response)

#         else:
#             print("No new messages to read from the queue.")
#             return False

