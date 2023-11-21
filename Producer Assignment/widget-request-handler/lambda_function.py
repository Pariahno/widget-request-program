import json
import widget_producer

def lambda_handler(event, context):
    
    event_body = event['body']
    processed_request = widget_producer.create_request(event['body'])
    
    return {
        'statusCode': 200,
        'body': json.dumps('Request sent successfully')
    }