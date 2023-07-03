import os
import subprocess

import boto3

s3_bucket = "mlops-task"
s3_folder = "scripts/"
s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

diff_output = (
    subprocess.check_output(["git", "diff", "--name-status", "HEAD~1..HEAD"])
    .decode()
    .strip()
)

for line in diff_output.split("\n"):
    status, file_path = line.split("\t")

    if "scripts/" not in file_path:
        continue
    print(f"File path: {file_path}")
    if status == "A" or status == "M":
        s3_key = s3_folder + file_path
        s3_client.upload_file(file_path, s3_bucket, s3_key)
        print(f"Uploaded file to S3: {s3_key}")
    elif status == "D":
        s3_key = s3_folder + file_path
        s3_client.delete_object(Bucket=s3_bucket, Key=s3_key)
        print(f"Deleted file from S3: {s3_key}")
