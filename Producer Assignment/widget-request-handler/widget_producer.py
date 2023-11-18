import boto3
import json

sqs_client = boto3.client('sqs')
queue_url = "https://sqs.us-east-1.amazonaws.com/888013785313/cs5260-requests" 

def create_request(input_data):
    return
