from __future__ import print_function

import json
import re
try:
    from urllib import quote_plus, unquote_plus
    from urlparse import urljoin
except ImportError:
    from urllib.parse import quote_plus, unquote_plus
    from urllib.parse import urljoin
try:
    from email.Utils import formatdate
except ImportError:
    from email.utils import formatdate
from xml.sax.saxutils import escape
from os import path

import boto3
from botocore.exceptions import ClientError

class LambdaTestButton(Exception):
    pass


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

DOMAIN = 'http://{bucket}.s3-{region}.amazonaws.com'
FEED_FILENAME = 'feed.xml'

TEST_BUCKET = 'sourcebucket'


def natural_key(string_):
    """Split string_ into number / letter words, so e.g. A2 is lower than A10

    From http://stackoverflow.com/a/3033342/15890"""
    return [int(s) if s.isdigit() else s for s in re.split(r'(\d+)', string_)]


def rssfeed(feed_data, items):
    item_xml = ''.join(
        ITEM_TEMPLATE.format(**item) for item in items
    )
    return FEED_TEMPLATE.format(items=item_xml, **feed_data)

def deltaed_date_as_str(base_date, delta):
    dsecs = delta * 24 * 60 * 60
    return formatdate(dsecs + float(base_date.strftime('%s')))


def episode_data(i, object_data, bucket, region):
    key = object_data['Key']
    fn = path.basename(key)
    title = path.splitext(fn)[0]
    filesize = object_data['Size']
    dt = object_data['LastModified']
    domain = DOMAIN.format(bucket=bucket, region=region)
    return {
        'title': escape(title),
        'url': urljoin(domain, quote_plus(key, safe='/')),
        'filesize': filesize,
        # dumb guess about duration
        'length_secs': filesize / 1500,
        'date': deltaed_date_as_str(dt, i),
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
        if obj['Key'].endswith(('.mp3', '.m4a', '.m4b'))
        if not obj['Key'].startswith('_')
    ]


def write_feed(bucket, folder, region):
    episode_data = get_episode_data(bucket, folder, region)
    feed_path = path.join(folder, FEED_FILENAME)
    domain = DOMAIN.format(bucket=bucket, region=region)
    encoded_path = quote_plus(feed_path, safe='/')
    feed_url = urljoin(domain, encoded_path)
    print(feed_path, feed_url)

    feed_data = {
        'title': escape(folder),
        'description': escape(folder),
        'url': feed_url,
        'path': feed_path,
        'encoded_path': encoded_path,
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
    feed_path = feed_data['encoded_path']
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
        raise LambdaTestButton
    else:
        if bucket == TEST_BUCKET:
            raise LambdaTestButton
        return bucket

def get_default_bucket():
    return [
        b['Name'] for b in s3.list_buckets()['Buckets']
        if 'podcast' in b['Name']][0]

def get_folders(event, bucket):
    print('get_folders')
    upload = event['Records'][0]['s3']
    key = unquote_plus(upload['object']['key'])
    print('Key={}'.format(key))
    folder = path.dirname(key)
    print('Folder={}'.format(folder))
    if folder:
        return {folder}
    key_data = s3.list_objects_v2(Bucket=bucket)
    keys = [k['Key'] for k in key_data['Contents']]
    print('keys={}'.format(keys))
    return {path.dirname(key) for key in keys if path.dirname(key)}

def get_region(event, is_test_button):
    if is_test_button:
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

    is_test_button = False
    try:
        bucket = get_bucket(event)
    except LambdaTestButton:
        is_test_button = True
        bucket = get_default_bucket()

    region = get_region(event, is_test_button)
    folders = get_folders(event, bucket)
    print('Folders={}'.format(folders))
    print('Region={}, Bucket={}'.format(region, bucket))
    log_data = {}
    for folder in folders:
        print('Folder={}'.format(folder))
        feed_data = write_feed(bucket, folder, region)
        write_index(bucket, feed_data)
        log_data[folder] = feed_data
    return log_data
