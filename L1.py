from botocore.vendored import requests
import json
import boto3
import random
import datetime

def getReferrer():
    x = random.randint(1,4)
    # x = 1
    x = x*40
    y = x+20 
    data = {}
    data['user_id'] = random.randint(x,y)
    # data['user_id'] = random.choice(list(range(1, 10)))
    data['device'] = random.choice(['mobile','computer', 'tablet', 'mobile','computer'])
    data['event'] = random.choice(['nav_menu','checkout', 'cart', 'product_detail', 'products', 'product_detail', 'nav_menu', 'orders_', 'contact_us', 'contact_us'])
    
    dt = datetime.datetime.now()
    timestamp = dt.isoformat()
    data['clicktimestamp'] = timestamp
    
    return data

def lambda_handler(event, context):
    # iterate = random.choice([800, 822, 844, 866, 888, 911, 933, 955, 977, 1000])
    
    kinesis = boto3.client('kinesis', region_name='us-east-1')
    start = datetime.datetime.now()
    new = start
    while (new - start).total_seconds()<50:
        data = json.dumps(getReferrer())
        data = str(data)+'\n'
        kinesis.put_record(
                StreamName='project-Stream',
                Data=data,
                PartitionKey='partitionkey')
        new = datetime.datetime.now()
    # des = kinesis.describe_stream(
    #         StreamName='project-Stream',
    #         Limit=1
    #         )
    # print(des)
    # print(int(iterate))
    # for i in range(int(iterate)):
    
    return True
    # for i in range(5):
    #     print(json.dumps(getReferrer()))


# def lambda_handler(event, context):
#     # TODO implement
#     return {
#         'statusCode': 200,
#         'body': json.dumps('Hello from Lambda!')
#     }
