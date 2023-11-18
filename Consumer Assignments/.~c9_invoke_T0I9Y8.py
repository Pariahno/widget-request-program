"""
Your module description here
"""
import argparse
import time
import json
import boto3

# Read and process command line arguments
ap = argparse.ArgumentParser()
ap.add_argument("-rb", "--request-bucket", help="source bucket of widget requests")
ap.add_argument("-wb", "--widget-bucket", help="destination bucket to send widgets")
ap.add_argument("-wt", "--widget-table", help="destination database table to send widgets")
args = ap.parse_args()

def main(args):
    rb_name = args.request_bucket
    wb_name = args.widget_bucket
    wt_name = args.widget_table

    s3_resource = boto3.resource('s3')
    dynamo_db_resource = boto3.resource('dynamodb')
    
    #TODO: REMOVE THIS
    print("-----------------------")
    
    # Checking for existing resources from arguments
    bucket_list = get_bucket_list(s3_resource)
    table_list = get_table_list(dynamo_db_resource)
    if rb_name == None or (wb_name == None and wt_name == None):
        print("Insufficient arguments specified")
        return
    elif not rb_name in bucket_list:
        print("Request bucket not found")
        return
    elif wb_name in bucket_list:
        rb = s3_resource.Bucket(rb_name)
        wb = s3_resource.Bucket(wb_name)
        read_requests(rb, wb=wb)
    elif wt_name in table_list:
        rb = s3_resource.Bucket(rb_name)
        wt = dynamo_db_resource.Table(wt_name)
        read_requests(rb, wt=wt)
    else:
        print("Resources not found")
       
    
# Reads objects from request bucket one at a time
# Stops when no more objects appear for a few seconds
def read_requests(rb, wb=None, wt=None):
    timer = 0
    while timer < 3000:
        if len(list(rb.objects.limit(count=1))) > 0:
            for object in rb.objects.limit(count=1):
                process_request(rb, wb, wt, object.key)
                rb.delete_objects(Delete={
                    'Objects': [{ 'Key': object.key }]
                    })
                timer = 0
        else:
            time.sleep(0.1) 
            timer += 100


# Gets object, read and parses it, then processes it
def process_request(rb, wb, wt, object_key):
    s3_client = boto3.client('s3')
    current_object = s3_client.get_object(
        Bucket=rb.name,
        Key=object_key)
    contents = current_object['Body'].read().decode('utf-8')
    object_dict = json.loads(contents)
    if wb != None:
        store_widget_in_bucket(s3_client, wb, object_dict)
    if wt != None:
        store_widget_in_table()
    

# Stores widget in S3 bucket
def store_widget_in_bucket(client, wb, widget_contents):
    widget_key = f"widgets/{widget_contents['owner']}/{widget_contents['widgetId']}"
    widget_body = json.dumps(widget_contents)
    current_widget = client.put_object(
        Body=widget_body,
        Bucket=wb.name,
        Key=widget_key)
        

def store_widget_in_table()
    

# Returns a list of strings of existing bucket names
def get_bucket_list(s3_resource):
    bucket_list = []
    for bucket in s3_resource.buckets.all():
        bucket_list.append(bucket.name)
    
    return bucket_list
    
# Returns a list of strings of existing table names
def get_table_list(db_resource):
    table_list = []
    for table in db_resource.tables.all():
        table_list.append(table.name)
        
    return table_list

main(args)




    