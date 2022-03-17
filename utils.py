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
# fifo queue
# queue_url = "https://sqs.us-east-1.amazonaws.com/247558419887/sqs-send.fifo"

# standard queues

#harish sqs
queue_url = "https://sqs.us-east-1.amazonaws.com/247558419887/cc-project-request-queue"
response_queue_url = "https://sqs.us-east-1.amazonaws.com/247558419887/cc-project-response-queue"

#suraj sqs
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



def receive_msg_and_delete_image():
    
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
                    print("Delete response : ",delete_response)
            #   img_name = response['MessageAttributes']['image_name']['StringValue']
            # print("encoded_img_data : ",img_data)
            # print("image name : ",img_name)
            # print("Decoding image data ....")
            # base64_img_bytes = img_data.encode('utf-8')
            # decoded_image_data = base64.decodebytes(base64_img_bytes)
            # try:
            #     image_result = open('decoded_'+img_name, 'wb')
            #     image_result.write(decoded_image_data)
            # except:
            #     print("Exception occured while decoding the image data!!!")
            #     print("Will retry after few seconds...!!!")
            #     raise
            # else:
            #     if len(message_receipt_handle) > 0:
            #         print("Deleting message with image name : ",img_name," ...")
            #         delete_response = sqs_client.delete_message(QueueUrl=queue_url,ReceiptHandle=message_receipt_handle)
            #         print("Delete response : ",delete_response)
        else:
            print("No new messages to read from the queue.")
            return False






# def push_to_response_queue(image_file_name, image_recog_result):
#     sqs_client = boto3.client('sqs', region_name='us-east-1')
#     json_str = '{ "img_name" : "'+image_file_name+'" , "img_output" : "'+image_recog_result+'" }'
#     try:
#         response = sqs_client.send_message(QueueUrl=response_queue_url, MessageBody=json_str, MessageAttributes={ 'image_name': { 'StringValue': image_file_name , 'DataType': 'String' } } )
#     except ClientError as e:
#         print("Exception occured while pushing output data to SQS queue!!!",e)
#         logging.error(e)
#         return False
#     else:
#         print("Image output of : ",image_file_name," pushed to SQS queue successfully.")