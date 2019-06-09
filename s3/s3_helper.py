import time
import datetime
import boto3
import config
import logging


def write_to_s3(message_value: str, s3_location: str, file_name: str):
    """
    Writes the specified data to s3
    Parameters
    --------
    message_value : Message/data that needs to be written to s3
    s3_location : Location of the file on s3 where the data needs to be saved
    file_name: The name of file that needs to be saved on s3

    Examples
    --------
    write_to_s3(file ,"mybucket/filename.json" )
    """

    message = message_value.encode()
    s3 = boto3.resource('s3')
    bucket = config.bucket_name
    logging.info('writing' + file_name + 'to s3.. at location'+ s3_location)
    data = s3_location + "/" + file_name
    s3object = s3.Object(bucket,data)
    s3object.put(Body=message)


