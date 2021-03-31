#!/usr/bin/env python

from optparse import OptionParser
import logging
import boto3
from botocore.exceptions import ClientError
import os
import datetime
import sys
from elasticsearch import Elasticsearch

logging.basicConfig(level=logging.INFO)

es = Elasticsearch()
s3 = boto3.client(
    's3',
    region_name='us-west-2',
    aws_access_key_id='',
    aws_secret_access_key=''
)

def list_objects(bucket, prefix):
  res = s3.list_objects(Bucket=bucket, Prefix=prefix, MaxKeys = 1000)
  objects = []
  for object in res['Contents']:
    objects.append(object['Key'])
  logging.info(objects)
  return objects

# https://elasticsearch-py.readthedocs.io/en/v7.12.0/
def es_index(index, bucket, objects):
  for object in objects:
    res = es.index(index=index, body= {
      'meta': {
        'type': 's3',
        'object': object,
        'bucket': bucket
      },
      'tag': 'rm'
    })

def main():
  parser = OptionParser(usage="usage: %prog [options]",
                        version="%prog 1.0")
  parser.add_option("-b", "--bucket",
                    dest="bucket",
                    default=False,
                    help="s3 bucket")
  parser.add_option("-p", "--prefix",
                    dest="prefix",
                    help="s3 prefix",)
  (options, args) = parser.parse_args()
  
  bucket = options.bucket
  prefix = options.prefix
  logging.info(f'bucket={bucket}, prefix={prefix}')
  
  objects = list_objects(bucket, prefix)
  es_index('log-poc', bucket, objects)

if __name__ == '__main__':
    main()