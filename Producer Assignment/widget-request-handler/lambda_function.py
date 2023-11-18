import json
import widget_producer

def lambda_handler(event, context):
    
    input_text = json.loads(event['body'])
    input_data = body.get('input_data', {})
    
    processed_request = widget_producer.create_request(input_data)
    
    return {
        'statusCode': 200,
        'body': json.dumps(input_data)
    }
