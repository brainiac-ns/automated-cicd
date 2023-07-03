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
    if line.startswith("A\t") or line.startswith("M\t") or line.startswith("D\t"):
        status, file_path = line.split("\t")
        files.append(line)

for file in files:
    status, file_path = file.split("\t")
    if file_path.startswith("scripts/"):
        if status == "A" or status == "M":
            s3_key = s3_folder + file_path
            s3_client.upload_file(file_path, s3_bucket, s3_key)
            print(f"Uploaded file to S3: {s3_key}")
        elif status == "D":
            s3_key = s3_folder + file_path
            s3_client.delete_object(Bucket=s3_bucket, Key=s3_key)
            print(f"Deleted file from S3: {s3_key}")
