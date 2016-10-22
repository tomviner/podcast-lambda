from __future__ import print_function

import json
import re
from urllib import quote_plus, unquote_plus
from email.Utils import formatdate
from os import path
from urlparse import urljoin

import boto3
from botocore.exceptions import ClientError

print('Loading function')

s3 = boto3.client('s3')

FEED_TEMPLATE = """
<rss xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd" version="2.0">
    <channel>
        <title>{title}</title>
        <description>{description}</description>
        <link>{url}</link>
        <language>en-us</language>{items}
    </channel>
</rss>
"""

ITEM_TEMPLATE = """
        <item>
            <title>{title}</title>
            <description />
            <enclosure url="{url}" type="audio/mpeg" length="{filesize}" />
            <itunes:duration>{length_secs}</itunes:duration>
            <pubDate>{date}</pubDate>
        </item>"""

DOMAIN = 'http://{bucket}.s3-website-{region}.amazonaws.com'
FEED_FILENAME = 'feed.xml'
TEST_BUCKET = 'sourcebucket'


def natural_key(string_):
    """From http://stackoverflow.com/a/3033342/15890"""
    return [int(s) if s.isdigit() else s for s in re.split(r'(\d+)', string_)]


def rssfeed(feed_data, items):
    item_xml = ''.join(
        ITEM_TEMPLATE.format(**item) for item in items
    )
    return FEED_TEMPLATE.format(items=item_xml, **feed_data)


def episode_data(i, object_data, bucket, region):
    key = object_data['Key']
    fn = path.split(key)[1]
    title = path.splitext(fn)[0]
    filesize = object_data['Size']
    dt = object_data['LastModified']
    domain = DOMAIN.format(bucket=bucket, region=region)
    return {
        'title': title,
        'url': urljoin(domain, quote_plus(key, safe='/')),
        'filesize': filesize,
        # dumb guess about duration
        'length_secs': filesize / 1500,
        'date': formatdate(float(dt.strftime('%s'))),
    }


def get_episode_data(bucket, folder, region):
    """Extract the following episode data:

    title, url, filesize, length_secs, date
    """
    folder = (folder.rstrip('/') + '/').lstrip('/')
    print('s3.list_objects_v2(Bucket={!r}, Prefix={!r})'.format(
        bucket, folder))
    data = s3.list_objects_v2(Bucket=bucket, Prefix=folder)
    episodes = sorted(
        data['Contents'],
        key=lambda x: natural_key(x['Key']),
        reverse=True,)
    return [
        episode_data(i, obj, bucket, region)
        for i, obj in enumerate(episodes)
        if obj['Key'] != folder
        if obj['Key'].endswith('.mp3')
        if not obj['Key'].startswith('_')
    ]


def write_feed(bucket, folder, region):
    episode_data = get_episode_data(bucket, folder, region)
    feed_path = path.join(folder, FEED_FILENAME)
    domain = DOMAIN.format(bucket=bucket, region=region)
    feed_url = urljoin(domain, quote_plus(feed_path, safe='/'))

    feed_data = {
        'title': folder,
        'description': folder,
        'url': feed_url,
        'path': feed_path,
    }
    feed = rssfeed(feed_data, episode_data)
    print(feed)
    s3.put_object(
        Bucket=bucket,
        Key=feed_path,
        Body=feed,
        ContentType='application/xml'
    )
    return feed_data


def write_index(bucket, feed_data):
    try:
        index = s3.get_object(
            Bucket=bucket,
            Key='feeds.json',)
        feed_index = json.load(index['Body'])
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            feed_index = {}
        else:
            raise e
    feed_path = feed_data['path']
    feed_index[feed_path] = feed_data
    s3.put_object(
        Bucket=bucket,
        Key='feeds.json',
        Body=json.dumps(feed_index, indent=4),
        ContentType='application/json'
    )
    index_template = """
    <html>
        <body>
            {}
        </body>
    </html>
    """
    feed_links = [
        '<li><a href="{0[url]}">{0[title]}</a></li>'.format(feed)
        for feed in feed_index.values()
    ]
    html = index_template.format('<br>\n'.join(feed_links))
    s3.put_object(
        Bucket=bucket,
        Key='index.html',
        Body=html,
        ContentType='text/html'
    )

def get_bucket(event):
    upload = event['Records'][0]['s3']
    try:
        bucket = upload['bucket']['name']
    except KeyError:
        pass
    if bucket != TEST_BUCKET:
        return bucket
    return [
        b['Name'] for b in s3.list_buckets()['Buckets']
        if 'podcast' in b['Name']][0]

def get_folders(event, bucket):
    upload = event['Records'][0]['s3']
    key = unquote_plus(upload['object']['key'].encode('utf8'))
    print('Key={}'.format(key))
    folder = path.split(key)[0]
    if folder:
        return {folder}
    keys = s3.list_objects_v2(Bucket=bucket)
    return {path.split(key)[0] for key in keys if path.split(key)[0]}

def get_region(event):
    return 'eu-west-1'
    return event['Records'][0]['awsRegion']

def lambda_handler(event, context):
    """Write an RSS Podcast Feed upon any change to mp3s on S3.

    - An mp3 file has just been uploaded / deleted
    - Extract the podcast name from the "folder"
    - Collect details from each mp3 in the folder:
        - Filename
        - Size
    - Generate RSS Feed XML
    - Write RSS Feed
    """
    print("Received event: {}".format(json.dumps(event, indent=2)))

    # Get the object from the event
    bucket = get_bucket(event)
    region = get_region(event)
    folders = get_folders(event, bucket)
    print('Region={}, Bucket={}'.format(region, bucket))
    log_data = {}
    for folder in folders:
        print('Folder={}'.format(folder))
        feed_data = write_feed(bucket, folder, region)
        write_index(bucket, feed_data)
        log_data[folder] = feed_data
    return log_data
