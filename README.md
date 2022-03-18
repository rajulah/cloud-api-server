# cloud-api-server
REST API server for handling image recognition API requests


# install required libraries with 
```pip3 install requirements.txt```
or

```
pip3 install fastapi
pip3 install uvicorn
pip3 install python-multipart
pip3 install boto3
pip3 install awscli
pip3 install redis
```

# Deploying - run this in the project root folder
```nohup uvicorn main:app --host 0.0.0.0 --port 3000 >> fastapiLogs.log 2>&1 &```
or
```nohup uvicorn main:app --workers 4 --host 0.0.0.0 --port 3000 >> fastapiLogs.log 2>&1 &```

# Accessing API docs in browser
```http://{ec2-public-url}:3000/docs```

-- Send workload generator API requests to
```http://{ec2-public-url}:3000/upload```