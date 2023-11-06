import unittest
import boto3
import moto
from consumer import(
    read_requests,
    store_widget_in_bucket,
    store_widget_in_table,
    delete_widget_from_bucket,
    delete_widget_from_table,
    update_widget_in_table,
    )
import json


class TestConsumer(unittest.TestCase):
    
    @moto.mock_s3
    def test_read_requests(self):
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='mock_request_bucket')
        s3.create_bucket(Bucket='mock_widget_bucket')
        mock_rb = boto3.resource('s3').Bucket('mock_request_bucket')
        mock_wb = boto3.resource('s3').Bucket('mock_widget_bucket')
        mock_request = {'type': 'create', 'requestId': '1234abcd', 'widgetId': 'abcd1234', 'owner': 'Bill'}
        s3.put_object(Bucket='mock_request_bucket', Key=mock_request['requestId'], Body=json.dumps(mock_request))
        
        #Gives time to process request and delete request from bucket
        import threading
        t = threading.Thread(target=read_requests, args=(mock_rb, mock_wb, None))
        t.start()
        t.join(timeout=5)

        requests = s3.list_objects(Bucket='mock_request_bucket')
        self.assertEqual(len(requests.get('Contents', [])), 0)
        
    @moto.mock_s3  
    def test_store_widget_in_bucket(self):
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='mock_widget_bucket')
        mock_wb = boto3.resource('s3').Bucket('mock_widget_bucket')

        mock_widget_contents = {'type': 'create', 'requestId': '1234abcd', 'widgetId': 'abcd1234', 'owner': 'Bill'}
        store_widget_in_bucket(mock_wb, mock_widget_contents, 'create')
        widgets = s3.list_objects(Bucket='mock_widget_bucket')
        self.assertEqual(len(widgets['Contents']), 1)
        self.assertEqual(widgets['Contents'][0]['Key'], 'widgets/Bill/abcd1234')
        
    @moto.mock_dynamodb  
    def test_store_widget_in_table(self):
        dynamodb = boto3.client('dynamodb', region_name='us-east-1')
        table_name = 'mock_widget_table'
        dynamodb.create_table(
            TableName=table_name,
            KeySchema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        mock_wt = boto3.resource('dynamodb').Table(table_name)

        mock_widget_contents = {
            'type': 'create', 
            'requestId': '1234abcd',
            'widgetId': 'abcd1234',
            'owner': 'Bill',
            "otherAttributes":[
                {'name':'color', 'value': 'red'},
                {'name': 'cost', 'value': '10.50'}
            ]
        }
        
        store_widget_in_table(mock_wt, mock_widget_contents)
        dynamodb_resource = boto3.resource('dynamodb')
        table = dynamodb_resource.Table(table_name)
        response = table.get_item(Key={'id': 'abcd1234'})
        self.assertIn('Item', response)
        self.assertEqual(response['Item']['owner'], 'Bill')
        self.assertEqual(response['Item']['color'], 'red')
        self.assertEqual(response['Item']['cost'], '10.50')
        
    @moto.mock_s3
    def test_delete_widget_from_bucket(self):
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='mock_widget_bucket')
        mock_wb = boto3.resource('s3').Bucket('mock_widget_bucket')

        mock_widget_contents = {'type': 'create', 'requestId': '1234abcd', 'widgetId': 'abcd1234', 'owner': 'Bill'}
        mock_other_widget_contents = {'type': 'create', 'requestId': '5678efgh', 'widgetId': '5678efgh', 'owner': 'Julie'}
        mock_delete_widget_request = {'type': 'delete', 'requestId': '1234abcd', 'widgetId': 'abcd1234', 'owner': 'Bill'}
        store_widget_in_bucket(mock_wb, mock_widget_contents, 'create')
        store_widget_in_bucket(mock_wb, mock_other_widget_contents, 'create')
        widgets = s3.list_objects(Bucket='mock_widget_bucket')
        self.assertEqual(len(widgets['Contents']), 2)
        delete_widget_from_bucket(mock_wb, mock_delete_widget_request)
        widgets = s3.list_objects(Bucket='mock_widget_bucket')
        self.assertEqual(len(widgets['Contents']), 1)
        self.assertEqual(widgets['Contents'][0]['Key'], 'widgets/Julie/5678efgh')
        
    @moto.mock_dynamodb
    def test_delete_widget_from_table(self):
        dynamodb = boto3.client('dynamodb', region_name='us-east-1')
        table_name = 'mock_widget_table'
        dynamodb.create_table(
            TableName=table_name,
            KeySchema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        mock_wt = boto3.resource('dynamodb').Table(table_name)

        mock_widget_contents = {
            'type': 'create', 
            'requestId': '1234abcd',
            'widgetId': 'abcd1234',
            'owner': 'Bill',
            "otherAttributes":[
                {'name':'color', 'value': 'red'},
                {'name': 'cost', 'value': '10.50'}
            ]
        }
        
        store_widget_in_table(mock_wt, mock_widget_contents)
        mock_delete_widget_request = {'type': 'delete', 'requestId': '1234abcd', 'widgetId': 'abcd1234', 'owner': 'Bill'}
        delete_widget_from_table(mock_wt, mock_delete_widget_request)
        dynamodb_resource = boto3.resource('dynamodb')
        table = dynamodb_resource.Table(table_name)
        response = table.get_item(Key={'id': 'abcd1234'})
        self.assertNotIn('Item', response)
        
    @moto.mock_s3
    def test_update_widget_in_bucket(self):
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='mock_widget_bucket')
        mock_wb = boto3.resource('s3').Bucket('mock_widget_bucket')

        mock_widget_contents = {'type': 'create', 'requestId': '1234abcd', 'widgetId': 'abcd1234', 'owner': 'Bill', 'color': 'red'}
        store_widget_in_bucket(mock_wb, mock_widget_contents, 'create')
        mock_update_widget_request = {'type': 'update', 'requestId': '1234abcd', 'widgetId': 'abcd1234', 'owner': 'Bill', 'color': 'blue'}
        store_widget_in_bucket(mock_wb, mock_update_widget_request, 'update')
        widgets = s3.list_objects(Bucket='mock_widget_bucket')
        self.assertEqual(len(widgets['Contents']), 1)
        self.assertEqual(widgets['Contents'][0]['Key'], 'widgets/Bill/abcd1234')
        
    @moto.mock_dynamodb
    def test_update_widget_in_table(self):
        dynamodb = boto3.client('dynamodb', region_name='us-east-1')
        table_name = 'mock_widget_table'
        dynamodb.create_table(
            TableName=table_name,
            KeySchema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        mock_wt = boto3.resource('dynamodb').Table(table_name)

        mock_widget_contents = {
            'type': 'create', 
            'requestId': '1234abcd',
            'widgetId': 'abcd1234',
            'owner': 'Bill',
            "otherAttributes":[
                {'name':'color', 'value': 'red'},
                {'name': 'cost', 'value': '10.50'}
            ]
        }
        
        store_widget_in_table(mock_wt, mock_widget_contents)
        
        mock_widget_update_request = {
            'type': 'create', 
            'requestId': '1234abcd',
            'widgetId': 'abcd1234',
            'owner': 'Bill',
            "otherAttributes":[
                {'name':'color', 'value': 'blue'}
            ]
        }
        
        update_widget_in_table(mock_wt, mock_widget_update_request)
        dynamodb_resource = boto3.resource('dynamodb')
        table = dynamodb_resource.Table(table_name)
        response = table.get_item(Key={'id': 'abcd1234'})
        self.assertIn('Item', response)
        self.assertEqual(response['Item']['owner'], 'Bill')
        self.assertEqual(response['Item']['color'], 'blue')
        self.assertNotIn(response['Item']['cost'], '10.50')

        


if __name__ == '__main__':
    unittest.main()
    