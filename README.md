# cloud-api-server
REST API server for handling image recognition API requests


# install required libraries with 
```
pip3 install requirements.txt
``
or 
```
pip3 install fastapi
pip3 install unicorn
pip3 install python-multipart
pip3 install boto3
pip3 install awscli
```

# Deploying - run this in the project root folder
```
nohup uvicorn main:app --host 0.0.0.0 --port 3000 >> fastapiLogs.log 2>&1 &
```

