import boto3
import pytest
from moto import mock_s3

from write_rss_feed import DOMAIN
import write_rss_feed


@pytest.fixture
def region():
    return 'us-east-1'


@pytest.fixture
def domain(region, bucket):
    return DOMAIN.format(bucket=bucket, region=region)


@pytest.fixture
def mocked_s3(region):
    with mock_s3():
        s3 = boto3.client('s3', region_name=region)
        write_rss_feed.s3 = s3
        yield s3


@pytest.fixture
def bucket():
    return 'my-bucket'


@pytest.fixture
def folder():
    return 'Pod-Fever'


@pytest.fixture
def folder2():
    return '2nd-podcast & friends'


@pytest.fixture
def uploaded_episodes(mocked_s3, bucket, folder):
    mocked_s3.create_bucket(Bucket=bucket)
    for i in range(1, 11):
        size = 1000 + 100 * i
        mocked_s3.put_object(
            Bucket=bucket,
            Key='{}/episode{}.mp3'.format(folder, i),
            Body='x' * size,)


@pytest.fixture
def event(bucket, folder):
    return make_event("{}/episode10.mp3".format(folder), bucket)


@pytest.fixture
def event2(bucket, folder2):
    return make_event("{}/talking-17.mp3".format(folder2), bucket)


def make_event(key, bucket):
    return {
        "Records": [
            {
                "eventVersion": "2.0",
                "eventTime": "1970-01-01T00:00:00.000Z",
                "requestParameters": {
                    "sourceIPAddress": "127.0.0.1"
                },
                "s3": {
                    "configurationId": "testConfigRule",
                    "object": {
                        "eTag": "0123456789abcdef0123456789abcdef",
                        "key": key,
                        "sequencer": "0A1B2C3D4E5F678901",
                        "size": 1024
                    },
                    "bucket": {
                        "ownerIdentity": {
                            "principalId": "EXAMPLE"
                        },
                        "name": bucket,
                        "arn": "arn:aws:s3:::mybucket"
                    },
                    "s3SchemaVersion": "1.0"
                },
                "responseElements": {
                    "x-amz-id-2": "EXAMPLE123/567/ABCDEFGH",
                    "x-amz-request-id": "EXAMPLE123456789"
                },
                "awsRegion": "us-east-1",
                "eventName": "ObjectCreated:Put",
                "userIdentity": {
                    "principalId": "EXAMPLE"
                },
                "eventSource": "aws:s3"
            }
        ]
    }
