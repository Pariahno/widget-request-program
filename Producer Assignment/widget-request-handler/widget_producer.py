import boto3
import json
from jsonschema import validate, ValidationError

sqs_client = boto3.client('sqs')
queue_url = "https://sqs.us-east-1.amazonaws.com/888013785313/cs5260-requests" 
request_schema = {
  "type": "object",
  "properties": {
    "type": {
      "type": "string",
      "pattern": "create|delete|update"
    },
    "requestId": {
      "type": "string"
    },
    "widgetId": {
      "type": "string"
    },
    "owner": {
      "type": "string",
      "pattern": "[A-Za-z ]+"
    },
    "label": {
      "type": "string"
    },
    "description": {
      "type": "string"
    },
    "otherAttributes": {
      "type": "array",
      "items": [
        {
          "type": "object",
          "properties": {
            "name": {
              "type": "string"
            },
            "value": {
              "type": "string"
            }
          },
          "required": [
            "name",
            "value"
          ]
        }
      ]
    }
  },
  "required": [
    "type",
    "requestId",
    "widgetId",
    "owner"
  ]
}

def create_request(input_data):
    input_json = json.loads(input_data)
    is_valid = validate_json(input_json)
    return is_valid

def validate_json(input_data):
    try:
        validate(instance=input_data, schema=request_schema)
        return True
    except ValidationError as e:
        return False
    