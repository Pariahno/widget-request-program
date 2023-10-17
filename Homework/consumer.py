"""
Your module description here
"""
import argparse
import boto3

ap = argparse.ArgumentParser()
ap.add_argument("-rb", "--request-bucket", help="source bucket of widget requests")
ap.add_argument("-wb", "--widget-bucket", help="destination bucket to send widgets")
ap.add_argument("-wt", "--widget-table", help="destination database table to send widgets")
args = ap.parse_args()

def main(args):
    rb_name = args.request_bucket
    wb_name = args.widget_bucket
    wt = args.widget_table

    s3_resource = boto3.resource('s3')
    
    
    print("-----------------------")
    
    
    bucket_list = get_bucket_list(s3_resource)
    if rb_name == None or (wb_name == None and wt == None):
        print("Insufficient arguments specified")
        return
    elif not rb_name in bucket_list:
        print("Request bucket not found")
        return
    elif wb_name in bucket_list:
        rb = s3_resource.Bucket(rb_name)
        wb = s3_resource.Bucket(wb_name)
        read_requests(rb, wb)
    else:
        print("Resources not found")
       
        
def read_requests(rb, wb):
    for object in rb.objects.limit(count=1):
        print(object.key)
    print(f"Request bucket: {rb}")
    print(f"Widget bucket: {wb}")


def get_bucket_list(s3_resource):
    bucket_list = []
    for bucket in s3_resource.buckets.all():
        bucket_list.append(bucket.name)
    
    return bucket_list

main(args)




    