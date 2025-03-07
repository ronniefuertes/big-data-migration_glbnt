import json
import os
import requests
import boto3

# Environment Variables (Set these in AWS Lambda)
API_URL = os.getenv("API_URL")  # Example: http://your-ec2-instance/api/upload_csv
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")  # Example: "your-raw-data-bucket"

s3_client = boto3.client("s3")

def lambda_handler(event, context):
    """
    AWS Lambda function triggered by S3 when a new CSV file is uploaded.
    Downloads the file and sends it to the API's /upload_csv endpoint.
    """
    try:
        # Extract S3 file details from the event
        for record in event["Records"]:
            s3_bucket = record["s3"]["bucket"]["name"]
            s3_key = record["s3"]["object"]["key"]

            print(f"New file detected: {s3_key} in bucket: {s3_bucket}")

            # Validate the file type (only process CSV files)
            if not s3_key.endswith(".csv"):
                print(f"Skipping non-CSV file: {s3_key}")
                continue

            # Download file from S3 to Lambda's /tmp directory
            local_file_path = f"/tmp/{os.path.basename(s3_key)}"
            s3_client.download_file(s3_bucket, s3_key, local_file_path)
            print(f"Downloaded {s3_key} to {local_file_path}")

            # Send file to API
            with open(local_file_path, "rb") as file_data:
                files = {"file": (os.path.basename(s3_key), file_data, "text/csv")}
                response = requests.post(f"{API_URL}", files=files)

            # Handle API response
            if response.status_code == 200:
                print(f"File {s3_key} successfully sent to API. Response: {response.json()}")
            else:
                print(f"Failed to upload {s3_key}. API responded with: {response.text}")

    except Exception as e:
        print(f"Error processing file: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Processing completed"})
    }
