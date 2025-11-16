#!/usr/bin/env python3

import boto3
import json

dynamodb = boto3.resource('dynamodb',endpoint_url='http://localhost:8000',
                  region_name='None', aws_access_key_id='cassandra', aws_secret_access_key='cassandra')

response = dynamodb.batch_get_item(RequestItems={ "usertable" : { "Keys": [{ "key": "test" }] } })

print(json.dumps(response, indent=2))
