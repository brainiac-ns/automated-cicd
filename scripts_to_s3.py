import subprocess

import boto3

s3_bucket = "mlops-task"
s3_folder = "scripts/"
s3_client = boto3.client("s3")

# Run 'git log' command to get the list of changed files in the latest commit
log_output = (
    subprocess.check_output(["git", "log", "--name-status", "-1", "HEAD"])
    .decode()
    .strip()
)

files = []
start_index = log_output.index("\n") + 1

for line in log_output[start_index:].split("\n"):
    if line.startswith("A") or line.startswith("M") or line.startswith("D"):
        status, file_path = line.split("\t")
        files.append(file_path)

for file_path in files:
    if file_path.startswith("scripts/"):
        status = file_path[0]

        if status == "A" or status == "M":
            # Upload file to S3
            s3_key = s3_folder + file_path
            s3_client.upload_file(file_path, s3_bucket, s3_key)
            print(f"Uploaded file to S3: {s3_key}")
        elif status == "D":
            # Delete file from S3
            s3_key = s3_folder + file_path
            s3_client.delete_object(Bucket=s3_bucket, Key=s3_key)
            print(f"Deleted file from S3: {s3_key}")
