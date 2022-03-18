import redis
import boto3
import time
import json

red = redis.Redis(
host= 'localhost',
port= '6379')

red.set('mykey', 'Hello from Python!')
value = red.get('mykey') 
print(value)

# red.zadd('vehicles', {'car' : 0})
# red.zadd('vehicles', {'bike' : 0})
# vehicles = red.zrange('vehicles', 0, -1)
# print(vehicles)

red.set('team', 'val')
red.set('hey','b')
value = red.get('team')
print(str(value))

red.delete('hey')
print(red.get('hey'))

# red.flushdb()
# print(red.get('team'))
# #harish sqs
# queue_url = "https://sqs.us-east-1.amazonaws.com/247558419887/cc-project-request-queue"
# response_queue_url = "https://sqs.us-east-1.amazonaws.com/247558419887/cc-project-response-queue"

# #suraj sqs

queue_url = "https://sqs.us-east-1.amazonaws.com/027200419369/ImageQueueStandard"
response_queue_url = "https://sqs.us-east-1.amazonaws.com/027200419369/resultQueueStandard"

def receive_messages_from_sqs():
    sqs_client = boto3.client('sqs', region_name='us-east-1')
    while True:
        try:
            response = sqs_client.receive_message(QueueUrl=response_queue_url,MaxNumberOfMessages=1, MessageAttributeNames=['All'])
        except Exception as e:
            print('Could not receive the message from the Queue!!!', e)
            raise
        else:
            if len(response.get("Messages", [])) > 0:
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

                # filename = str(img_name).replace('.jpg', '').strip()
                # output_val = str(json_data['img_output'])
                # output_val = output_val.strip("\n")

                # self.dict[filename] = output_val
                red.set(filename,output_val)

            else:
                time.sleep(3)
                print(" --- No new messages to read from the queue.")

receive_messages_from_sqs()