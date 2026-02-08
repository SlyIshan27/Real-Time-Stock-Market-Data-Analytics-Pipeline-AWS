import json
import boto3
from decimal import Decimal

#This is the just code for my second Lambda function that sends data from a DynamoDB stream to a SNS topic.

sns = boto3.client('sns')
topicArn = "arn:aws:sns:us-east-2:912163986341:SendStockData"

def lambda_handler(event, context):
    for record in event['Records']:
        # print(record['dynamodb']['NewImage'])
        if record['eventName'] != 'INSERT':
            continue
        newImage = record['dynamodb']['NewImage']
        symbol = newImage['stockSymbol']['S']
        timestamp = newImage['timestamp']['S']
        closePrice = newImage['closePrice']['N']

        sns.publish(
            TopicArn=topicArn,
            Message=json.dumps({
                'stockSymbol': symbol,
                'timestamp': timestamp,
                'closePrice': closePrice,
                'Event' : "Stock Data from DynamoDB."
            }))
        print("Sent data to SNS")
    return {
        'statusCode': 200,
    }
