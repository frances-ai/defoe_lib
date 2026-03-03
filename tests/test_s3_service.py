import os
import uuid

import boto3
import pytest

import s3_service


@pytest.mark.integration
def test_write_and_read_text_lines_in_s3_real_client():
    object_key = f"s3-service-{uuid.uuid4().hex}.yml"
    expected_payload = "sugar candy\ncandy\nlove"
    expected_lines = ["sugar candy", "candy", "love"]

    original_bucket_name = s3_service.bucket_name
    s3_client = s3_service.s3

    try:
        s3_service.write_data_to_s3(expected_payload, object_key)
        actual_payload = s3_service.read_data_in_s3(object_key).decode("utf-8")
        assert actual_payload == expected_payload
        lines = actual_payload.split("\n")
        assert lines == expected_lines
    finally:
        s3_client.delete_object(Bucket=original_bucket_name, Key=object_key)
