import argparse
import sys
import time
import json
import boto3
import logging

#Set up logging
logging.basicConfig(filename="consumer.log", level=logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

# Read and process command line arguments
ap = argparse.ArgumentParser()
ap.add_argument("-rb", "--request-bucket", help="source bucket of widget requests")
ap.add_argument("-rq", "--request-queue", help="source queue of widget requests")
ap.add_argument("-wb", "--widget-bucket", help="destination bucket to send widgets")
ap.add_argument("-wt", "--widget-table", help="destination database table to send widgets")
args = ap.parse_args()

def main(args):
    rb_name = args.request_bucket
    rq_name = args.request_queue
    wb_name = args.widget_bucket
    wt_name = args.widget_table

    s3_resource = boto3.resource('s3')
    sqs_resource = boto3.resource('sqs')
    dynamo_db_resource = boto3.resource('dynamodb')
    
    # Checking for existing resources from arguments
    bucket_list = get_bucket_list(s3_resource)
    rq_url = get_queue_url(sqs_resource, rq_name)
    table_list = get_table_list(dynamo_db_resource)
    if (rb_name == None and rq_name == None) or (wb_name == None and wt_name == None):
        logging.error('Insufficient arguments specified')
        print("Insufficient arguments specified")
        return
    elif (not rb_name == None and not rb_name in bucket_list) or (not rq_name == None and rq_url == ''):
        logging.error('Request resource not found')
        print("Request resource not found")
        return
    elif (not wb_name == None and not wb_name in bucket_list) or (not wt_name == None and not wt_name in table_list):
        logging.error('Destination resources not found')
        print("Destination resources not found")
        return
    else:
        rb = None
        rq = None
        wb = None
        wt = None
        if rb_name in bucket_list:
            rb = s3_resource.Bucket(rb_name)
        if rq_url != '':
            rq = sqs_resource.Queue(rq_url)
        if wb_name in bucket_list:
            wb = s3_resource.Bucket(wb_name)
        if wt_name in table_list:
            wt = dynamo_db_resource.Table(wt_name)
        read_requests(rb, rq, wb, wt)
       
    

def read_requests(rb, rq, wb, wt):
    # Reads objects from request bucket one at a time
    # Stops when no more objects appear for a few seconds
    if rb != None:
        timer = 0
        while timer < 5000:
            if len(list(rb.objects.limit(count=1))) > 0:
                for object in rb.objects.limit(count=1):
                    logging.info(f'Read request {object.key} from bucket {rb.name}')
                    process_request_from_bucket(object.key, rb, wb, wt)
                    timer = 0
            else:
                time.sleep(0.1) 
                timer += 100
        logging.info('Timed out from reading requests from bucket')
        
    #Reads objects from request queue, caching 10 at a time and processing them one by one
    #Stops when no more messages appear for several seconds
    if rq != None:
        queue_empty = False
        while not queue_empty:
            messages = rq.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=5)
            process_requests_from_queue(messages, wb, wt)
            if len(messages) == 0:
                queue_empty = True
        logging.info('Timed out from reading requests from queue')
        

#Processes request recieved from an S3 bucket and returns a dictionary
def process_request_from_bucket(object_key, rb, wb, wt):
    s3_client = boto3.client('s3')
    current_object = s3_client.get_object(
        Bucket=rb.name,
        Key=object_key)
    contents = current_object['Body'].read().decode('utf-8')
    process_request_type(contents, wb, wt)
    rb.delete_objects(Delete={
        'Objects': [{ 'Key': object_key }]
        })
    logging.info(f'Deleted request {object_key} from bucket {rb.name}')
    
    
#Processes list of requests received from SQS and passes them to destination bucket/table
def process_requests_from_queue(messages, wb, wt):
    for m in messages:
        contents = m.body
        process_request_type(contents, wb, wt)
        m.delete()
        object_dict = json.loads(contents)
        logging.info(f"Deleted request {object_dict['requestId']} from queue")
  
        
#Given request contents, determines if is create, delete, or update and calls appropriate functions
def process_request_type(widget_contents, wb, wt):
    object_dict = json.loads(widget_contents)
    logging.info(f"Processed request {object_dict['requestId']}")
    request_type = object_dict['type']
    if request_type == 'create':
        if wb != None:
            store_widget_in_bucket(wb, object_dict, request_type)
        if wt != None:
            store_widget_in_table(wt, object_dict)
    elif request_type == 'delete':
        if wb != None:
            delete_widget_from_bucket(wb, object_dict)
        if wt != None:
            delete_widget_from_table(wt, object_dict)
    elif request_type == 'update':
        if wb != None:
            store_widget_in_bucket(wb, object_dict, request_type)
        if wt != None:
            update_widget_in_table(wt, object_dict)


# Stores widget in S3 bucket. Will also update an object if an object of the same key already exists
def store_widget_in_bucket(wb, widget_contents, request_type):
    widget_key = f"widgets/{widget_contents['owner']}/{widget_contents['widgetId']}"
    object_exists = True #True if create new widget. If otherwise updating, checks if object already exists
    if request_type == 'update':
        object_exists = check_bucket_object_exists(wb.name, widget_key)
    if object_exists:
        widget_body = json.dumps(widget_contents)
        current_widget = wb.put_object(
            Body=widget_body,
            Key=widget_key)
        if request_type == 'create':
            logging.info(f'Widget {widget_key} placed into bucket {wb.name}')
        elif request_type == 'update':
            logging.info(f'Widget {widget_key} updated in bucket {wb.name}')
    else:
        logging.warning(f'Widget {widget_key} not found to update in bucket {wb.name}')


# Stores widget in DynamoDB table
def store_widget_in_table(wt, widget_contents):
    if 'otherAttributes' in widget_contents:
        for attribute in widget_contents['otherAttributes']:
            att_name = attribute['name']
            att_value = attribute['value']
            widget_contents[att_name] = att_value
        del widget_contents['otherAttributes']
    widget_contents['id'] = widget_contents.pop('widgetId')
    current_widget = wt.put_item(Item=widget_contents)
    logging.info(f'Widget {widget_contents["id"]} placed into table {wt.name}')
    
    
# Deletes widget from S3 bucket, if it is found
def delete_widget_from_bucket(wb, widget_contents):
    widget_key = f"widgets/{widget_contents['owner']}/{widget_contents['widgetId']}"
    object_exists = check_bucket_object_exists(wb.name, widget_key)
    if object_exists:
        response = wb.delete_objects(
            Delete={
                'Objects': [
                    {
                       'Key': widget_key 
                    }
                ]
            }
        )
        logging.info(f'Deleted widget {widget_key} from bucket {wb.name}')
    else:
        logging.warning(f'Widget {widget_key} not found to delete from bucket {wb.name}')


# Deletes widget from DynamoDB table, if it is found
def delete_widget_from_table(wt, widget_contents):
    widget_id = widget_contents['widgetId']
    try:
        response = wt.delete_item(
            Key={
                'id': widget_id
            },
            ConditionExpression='attribute_exists(id)'
        )
        logging.info(f'Deleted widget {widget_id} from table {wt.name}')
    except wt.meta.client.exceptions.ConditionalCheckFailedException as e:
        logging.warning(f'Widget {widget_id} not found to delete from table {wt.name}')
  
  
#Updates a widget's attributes in a table, if it is found 
def update_widget_in_table(wt, widget_contents):
    widget_id = widget_contents['widgetId']
    attributes_to_update = widget_contents.get('otherAttributes', {})
    remove_attribute_list = [] #Used to join strings of attributes to remove
    update_attribute_list = [] #Used to join strings of attributes to be updated
    update_expression_list = [] #Used to join the two larger strings into isolated segments in case no REMOVE or SET operation is needed
    expression_attribute_names = {}
    expression_attribute_values = {}
    
    for attribute in attributes_to_update:
        name = attribute['name'].replace('-', '_') #Hyphens resulted in invalid key, replace with underscores
        value = attribute['value']
        
        if value == None:
            remove_attribute_list.append(f'#{name}')
        else:
            update_attribute_list.append(f'#{name}=:{name}')
            expression_attribute_names[f"#{name}"] = f"attributes.{name}"
            expression_attribute_values[f":{name}"] = value
    
    #Joining together strings into total update expression
    if len(remove_attribute_list) != 0:
        update_expression_list.append('REMOVE ' + (', '.join(remove_attribute_list)) + ' ')
    if len(update_attribute_list) != 0:
        update_expression_list.append('SET ' + (', '.join(update_attribute_list)))
    update_expression = ' '.join(update_expression_list)
    
    
    try:
        updated_widget = wt.update_item(
            Key={'id': widget_id},
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=expression_attribute_values,
            ConditionExpression='attribute_exists(id)'
        )
        logging.info(f'Updated widget {widget_id} in table {wt.name}')
    except wt.meta.client.exceptions.ConditionalCheckFailedException as e:
        logging.warning(f'Widget {widget_id} not found to update in table {wt.name}')


# Returns a list of strings of existing bucket names
def get_bucket_list(s3_resource):
    bucket_list = []
    for bucket in s3_resource.buckets.all():
        bucket_list.append(bucket.name)

    return bucket_list
    
    
# Finds url from sqs client through given queue name in arguments
def get_queue_url(sqs_resource, queue_name):
    url = ''
    if queue_name != None:
        sqs_client = boto3.client('sqs')
        response = sqs_client.list_queues(QueueNamePrefix=queue_name)
        if 'QueueUrls' in response:
            url_list = response['QueueUrls']
            if len(url_list) == 1:
                url = url_list[0]
            
    return url
    
    
# Returns a list of strings of existing table names
def get_table_list(db_resource):
    table_list = []
    for table in db_resource.tables.all():
        table_list.append(table.name)
        
    return table_list
    
    
# Returns boolean of whether given widget exists in bucket or not
def check_bucket_object_exists(bucket_name, widget_key):
    s3_client = boto3.client('s3')
    object_exists = False
    try:
        s3_client.head_object(Bucket=bucket_name, Key=widget_key)
        object_exists = True
    except s3_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            object_exists = False
        
    return object_exists


main(args)