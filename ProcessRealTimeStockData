import json
import boto3
import base64 
import uuid
from decimal import Decimal

#This is code for my first Lambda function which processes data from the Kinesis Data Stream, and then sends it to S3 and DynamoDB.

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('ProccessedStockData')

s3 = boto3.client('s3')
BUCKETNAME = "real-time-stock-data-ishan"
def lambda_handler(event, context):
    for record in event['Records']:
        payloadStr = base64.b64decode(record['kinesis']['data']).decode('utf-8') 
        UID = uuid.uuid4()   
        fileName = f"AppleStockData-{UID}.json"   
        payload = json.loads(payloadStr) 
        timestamp = payload["timestamp"]
        symbol = payload["symbol"]
        closePrice = Decimal(str(payload["close"]))
        s3.put_object(
            Bucket=BUCKETNAME,
            Key=fileName,
            Body=payloadStr
        )
        table.put_item(
            Item={
                'id': str(UID),
                'stockSymbol': symbol,
                'timestamp': timestamp,
                'closePrice': closePrice,
            }
        )
        print("Process data sent to DynamoDB and raw data sent to S3.")
        

    return {'statusCode': 200,}
