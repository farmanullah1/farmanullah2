import boto3
import os

def lambda_handler(event, context):
    # AWS S3 Configuration
    bucket_name = 'farmanullah-ansari'
    output_folder = '/tmp/output'  # Use /tmp for writable storage in Lambda
    merged_filename = 'merged_output.txt'

    # S3 Client
    s3 = boto3.client('s3')

    # Paths to the files in the S3 bucket
    csv_file_path = 'csvData.csv'
    json_file_path = 'jsonData.json'
    sql_file_path = 'sqlData.sql'

    # Create output directory if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Download the files from S3
    s3.download_file(bucket_name, csv_file_path, os.path.join(output_folder, csv_file_path))
    s3.download_file(bucket_name, json_file_path, os.path.join(output_folder, json_file_path))
    s3.download_file(bucket_name, sql_file_path, os.path.join(output_folder, sql_file_path))

    # Paths to the downloaded files
    csv_local_path = os.path.join(output_folder, csv_file_path)
    json_local_path = os.path.join(output_folder, json_file_path)
    sql_local_path = os.path.join(output_folder, sql_file_path)

    # Merged output file path
    merged_output_path = os.path.join(output_folder, merged_filename)

    # Merge the files
    with open(merged_output_path, 'w') as merged_file:
        # Append CSV content
        with open(csv_local_path, 'r') as csv_file:
            merged_file.write("CSV Data:\n")
            merged_file.write(csv_file.read())
            merged_file.write("\n\n")
        
        # Append JSON content
        with open(json_local_path, 'r') as json_file:
            merged_file.write("JSON Data:\n")
            merged_file.write(json_file.read())
            merged_file.write("\n\n")
        
        # Append SQL content
        with open(sql_local_path, 'r') as sql_file:
            merged_file.write("SQL Data:\n")
            merged_file.write(sql_file.read())
            merged_file.write("\n\n")

    # Upload the merged file back to S3
    s3.upload_file(merged_output_path, bucket_name, f'output/{merged_filename}')

    print(f"Merged file uploaded to S3 bucket '{bucket_name}' in the 'output' folder.")
    return {
        'statusCode': 200,
        'body': f'Merged file uploaded to S3 bucket {bucket_name} in the output folder.'
    }
