import json
import csv
import boto3

s3_client = boto3.client('s3')

def extract_data_from_s3(bucket_name, csv_key, json_key, sql_key):
    csv_data, json_data, sql_data = [], [], []

    # Extract CSV Data
    csv_obj = s3_client.get_object(Bucket=bucket_name, Key=csv_key)
    csv_lines = csv_obj['Body'].read().decode('utf-8').splitlines()
    reader = csv.DictReader(csv_lines)
    for row in reader:
        csv_data.append(row)

    # Extract JSON Data
    json_obj = s3_client.get_object(Bucket=bucket_name, Key=json_key)
    json_data = json.loads(json_obj['Body'].read().decode('utf-8'))

    # Extract SQL Data (Assuming it's a SQL dump file)
    sql_obj = s3_client.get_object(Bucket=bucket_name, Key=sql_key)
    sql_data = sql_obj['Body'].read().decode('utf-8')

    return csv_data, json_data, sql_data

def transform_data(csv_data, json_data, sql_data):
    # Combine data from CSV, JSON, and SQL (This is a placeholder transformation)
    transformed_data = {
        "csv": csv_data,
        "json": json_data,
        "sql": sql_data.splitlines()
    }
    return transformed_data

def load_transformed_data_to_s3(transformed_data, bucket_name, output_key):
    output_json = json.dumps(transformed_data)
    s3_client.put_object(Bucket=bucket_name, Key=output_key, Body=output_json)

def lambda_handler(event, context):
    bucket_name = 'farmanullah'
    csv_key = 'bankData.csv'
    json_key = 'jsonData.json'
    sql_key = 'sqlData.sql'
    output_key = 'transformed_data.json'

    # ETL Process
    csv_data, json_data, sql_data = extract_data_from_s3(bucket_name, csv_key, json_key, sql_key)
    transformed_data = transform_data(csv_data, json_data, sql_data)
    load_transformed_data_to_s3(transformed_data, bucket_name, output_key)

    return {
        'statusCode': 200,
        'body': json.dumps('ETL Process Completed')
    }
