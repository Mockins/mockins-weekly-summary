import boto3
import os

profile = os.getenv("AWS_POFILE", "mockins")
region = os.getenv("AWS_REGION", "us-east-1")

session = boto3.Session(profile_name=profile, region_name=region)

sts = session.client("sts")
identity = sts.get_caller_identity()

print("STS identity check passed:")
print(identity)