from smart_open import smart_open

import boto3
import tap_s3_csv.csv_handler
import tap_s3_csv.excel_handler


def get_file_handle(config, s3_path):
    bucket = config['bucket']
    s3_uri = f"s3://{bucket}/{s3_path}"
    return open_file_for_streaming(s3_uri, 'r', newline='', errors='surrogateescape')

# def get_file_handle(config, s3_path):
#     bucket = config['bucket']
#     s3_client = boto3.resource('s3')
#     s3_bucket = s3_client.Bucket(bucket)
#     s3_object = s3_bucket.Object(s3_path)
#     return s3_object.get()['Body']

def open_file_for_streaming(*args, **kwargs):    
    return smart_open(*args, **kwargs)

def get_row_iterator(config, table_spec, s3_path):
    file_handle = get_file_handle(config, s3_path)

    return get_filetype_handler(table_spec, file_handle)

def get_filetype_handler(table_spec, file_handle):

    if table_spec['format'] == 'csv':
        return tap_s3_csv.csv_handler.get_row_iterator(
            table_spec, file_handle)

    elif table_spec['format'] == 'excel':
        return tap_s3_csv.excel_handler.get_row_iterator(
            table_spec, file_handle)

