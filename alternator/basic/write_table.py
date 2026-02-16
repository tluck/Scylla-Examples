#!/usr/bin/env python3
import boto3
dynamodb = boto3.resource('dynamodb',endpoint_url='http://localhost:8000',
                  region_name='None', aws_access_key_id='cassandra', aws_secret_access_key='cassandra')

dynamodb.batch_write_item(RequestItems={
    'usertable': [
        {
             'PutRequest': {
                 'Item': {
                     'key': 'test', 'x' : {'hello': 'world'}
                 }
             },
        }
    ]
})
