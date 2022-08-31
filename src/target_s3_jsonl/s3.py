#!/usr/bin/env python3
import os
import backoff
import boto3
from botocore.exceptions import ClientError

from target_s3_jsonl.logger import get_logger

LOGGER = get_logger()


def retry_pattern():
    return backoff.on_exception(
        backoff.expo,
        ClientError,
        max_tries=5,
        on_backoff=log_backoff_attempt,
        factor=10)


def log_backoff_attempt(details):
    LOGGER.info("Error detected communicating with Amazon, triggering backoff: %d try", details.get("tries"))


@retry_pattern()
def create_client(config):
    LOGGER.info("Attempting to create AWS session")

    # Get the required parameters from config file and/or environment variables
    aws_access_key_id = config.get('aws_access_key_id') or os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = config.get('aws_secret_access_key') or os.environ.get('AWS_SECRET_ACCESS_KEY')
    aws_session_token = config.get('aws_session_token') or os.environ.get('AWS_SESSION_TOKEN')
    aws_profile = config.get('aws_profile') or os.environ.get('AWS_PROFILE')
    aws_endpoint_url = config.get('aws_endpoint_url')
    role_arn = config.get('role_arn')

    endpoint_params = {'endpoint_url': aws_endpoint_url} if aws_endpoint_url else {}

    # AWS credentials based authentication
    if aws_access_key_id and aws_secret_access_key:
        aws_session = boto3.session.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token)
    # AWS Profile based authentication
    else:
        aws_session = boto3.session.Session(profile_name=aws_profile)

    # config specifies a particular role to assume
    # we create a session & s3-client with this role
    if role_arn:
        role_name = role_arn.split('/', 1)[1]
        sts = aws_session.client('sts', **endpoint_params)
        resp = sts.assume_role(RoleArn=role_arn, RoleSessionName=f'role-name={role_name}-profile={aws_profile}')
        credentials = {
            "aws_access_key_id": resp["Credentials"]["AccessKeyId"],
            "aws_secret_access_key": resp["Credentials"]["SecretAccessKey"],
            "aws_session_token": resp["Credentials"]["SessionToken"],
        }
        aws_session = boto3.Session(**credentials)
        LOGGER.info(f"Creating s3 client with role {role_name}")
    return aws_session.client('s3', **endpoint_params)


# pylint: disable=too-many-arguments
@retry_pattern()
def upload_file(s3_client, filename, bucket, s3_key,
                encryption_type=None, encryption_key=None):

    if encryption_type is None or encryption_type.lower() == "none":
        # No encryption config (defaults to settings on the bucket):
        encryption_desc = ""
        encryption_args = None
    elif encryption_type.lower() == "kms":
        if encryption_key:
            encryption_desc = " using KMS encryption key ID '{}'".format(encryption_key)
            encryption_args = {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": encryption_key}
        else:
            encryption_desc = " using default KMS encryption"
            encryption_args = {"ServerSideEncryption": "aws:kms"}
    else:
        raise NotImplementedError(
            "Encryption type '{}' is not supported. "
            "Expected: 'none' or 'KMS'"
            .format(encryption_type))

    LOGGER.info("Uploading {} to bucket {} at {}{}".format(filename, bucket, s3_key, encryption_desc))
    s3_client.upload_file(filename, bucket, s3_key, ExtraArgs=encryption_args)
