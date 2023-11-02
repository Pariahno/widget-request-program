import argparse
import time
import json
import boto3
import logging

#Set up logging
logging.basicConfig(filename="consumer.log", level=logging.INFO)

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
    queue_list = get_queue_list(sqs_resource)
    table_list = get_table_list(dynamo_db_resource)
    if (rb_name == None and rq_name == None) or (wb_name == None and wt_name == None):
        logging.error('Insufficient arguments specified')
        print("Insufficient arguments specified")
        return
    elif (not rb_name == None and not rb_name in bucket_list) or (not rq_name == None and not rq_name in queue_list):
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
        if rq_name in queue_list:
            rq = sqs_resource.Queue(rq_name)
        if wb_name in bucket_list:
            wb = s3_resource.Bucket(wb_name)
        if wt_name in table_list:
            wt = dynamo_db_resource.Table(wt_name)
        read_requests(rb, rq, wb, wt)
       
    
# Reads objects from request bucket one at a time
# Stops when no more objects appear for a few seconds
def read_requests(rb, rq, wb, wt):
    if rb != None:
        timer = 0
        while timer < 3000:
            if len(list(rb.objects.limit(count=1))) > 0:
                for object in rb.objects.limit(count=1):
                    process_request(rb, None, wb, wt, object.key)
                    logging.info(f'Read request {object.key} from bucket {rb.name}')
                    rb.delete_objects(Delete={
                        'Objects': [{ 'Key': object.key }]
                        })
                    logging.info(f'Deleted request {object.key} from bucket {rb.name}')
                    timer = 0
            else:
                time.sleep(0.1) 
                timer += 100
        logging.info('Timed out from reading requests')
        
    #TODO: Enable reading and processing from the queue
        
    if rq != None:
        process_request(None, rq, wb, wt, object.key)
        


# Gets object, read and parses it, then processes it
def process_request(rb, rq, wb, wt, object_key):
    if rb != None:
        s3_client = boto3.client('s3')
        current_object = s3_client.get_object(
            Bucket=rb.name,
            Key=object_key)
    
    #TODO: Enable reading and processing from queue
    
    elif rq != None:
        sqs_client = boto3.client('sqs')
        
    else:
        logging.error('Enountered an error processing requests from source')
        return
    contents = current_object['Body'].read().decode('utf-8')
    object_dict = json.loads(contents)
    logging.info(f'Processed request {object_key}')
    if wb != None:
        store_widget_in_bucket(wb, object_dict)
    if wt != None:
        store_widget_in_table(wt, object_dict)

# Stores widget in S3 bucket
def store_widget_in_bucket(wb, widget_contents):
    widget_key = f"widgets/{widget_contents['owner']}/{widget_contents['widgetId']}"
    widget_body = json.dumps(widget_contents)
    current_widget = wb.put_object(
        Body=widget_body,
        Key=widget_key)
    logging.info(f'Widget {widget_key} placed into bucket {wb.name}')
        

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
        

# Returns a list of strings of existing bucket names
def get_bucket_list(s3_resource):
    bucket_list = []
    for bucket in s3_resource.buckets.all():
        bucket_list.append(bucket.name)

    return bucket_list
    
def get_queue_list(sqs_resource):
    queue_list = []
    for queue in sqs_resource.queues.all():
        queue_list.append(queue.name)
        
    return queue_list
    
    
# Returns a list of strings of existing table names
def get_table_list(db_resource):
    table_list = []
    for table in db_resource.tables.all():
        table_list.append(table.name)
        
    return table_list


main(args)




    