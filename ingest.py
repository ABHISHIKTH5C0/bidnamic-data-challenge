import io
import requests
import pandas as pd
import json
import boto3
from io import StringIO


def df_builder(url):
    s = requests.get(url).content
    df_pd = pd.read_csv(io.StringIO(s.decode('utf-8')))  # creted df using pandas
    return df_pd


campaigns = df_builder('https://raw.githubusercontent.com/bidnamic/bidnamic-data-challenge/master/campaigns.csv')

adgroups = df_builder('https://raw.githubusercontent.com/bidnamic/bidnamic-data-challenge/master/adgroups.csv')

search_terms = df_builder('https://raw.githubusercontent.com/bidnamic/bidnamic-data-challenge/master/search_terms.csv')

aws_config = json.load(open('aws_cofig.json'))

client = boto3.client('s3',
                      aws_access_key_id=aws_config['credential']['access_key'],
                      aws_secret_access_key=aws_config['credential']['secret_key'])

response = client.list_buckets()

bucket = aws_config['s3']['bucket']


def ingest_to_s3(client, df, bucket, filepath):
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepath)
    print(f'Copy {df.shape[0]} rows to S3 Bucket {bucket} at {filepath}, Done!')


ingest_to_s3(client=client, df=campaigns, bucket=bucket, filepath='campaigns.csv')
ingest_to_s3(client=client, df=adgroups, bucket=bucket, filepath='adgroups.csv')
ingest_to_s3(client=client, df=search_terms, bucket=bucket, filepath='search_terms.csv')
