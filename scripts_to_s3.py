import os

import boto3


# TODO: Razbiti na klasu i funkcije i napisati testove unit i integracione
def manage_files(bucket_name, folder):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder)

    if "Contents" in response:
        s3_files = [
            obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".py")
        ]
        local_files = []

        for file_name in os.listdir(folder):
            if file_name.endswith(".py"):
                local_files.append(os.path.join(folder, file_name))

        files_to_delete = set(s3_files) - set(local_files)

        for file in set(local_files):
            # TODO: Ovo samo za fajlove koji su promenjeni
            s3_client.upload_file(file, bucket_name, file)
            print(f"Uploaded file to S3: {file}")

        for file in files_to_delete:
            s3_client.delete_object(Bucket=bucket_name, Key=file)
            print(f"Deleted file from S3: {file}")


if __name__ == "__main__":
    bucket_name = "mlops-task"
    folder = "scripts"

    manage_files(bucket_name, folder)
