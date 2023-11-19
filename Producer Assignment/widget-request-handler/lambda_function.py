import json
import widget_producer

def lambda_handler(event, context):
    
    processed_request = widget_producer.create_request(event)
    
    return {
        'statusCode': 200,
        'body': json.dumps(event)
    }
