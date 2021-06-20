try:
    from urllib import quote_plus
except ImportError:
    from urllib.parse import quote_plus
from xml.sax.saxutils import escape
from write_rss_feed import FEED_FILENAME, lambda_handler


def test_rss_feed_written(
        mocked_s3, uploaded_episodes, bucket, folder, domain, event):
    lambda_handler(event, context=None)

    feed_path = '{}/feed.xml'.format(folder)
    feed_xml = mocked_s3.get_object(
        Bucket=bucket,
        Key=feed_path)['Body'].read().decode()
    mp3s = [
        e['Key']
        for e in mocked_s3.list_objects_v2(Bucket=bucket)['Contents']
        if e['Key'].endswith('.mp3')
    ]
    for mp3 in mp3s:
        enclosure = '<enclosure url="{}/{}"'.format(
            domain, quote_plus(mp3, safe='/'))
        assert enclosure in feed_xml


def test_index_with_first_feed(
        mocked_s3, uploaded_episodes, bucket, folder, domain, event):
    lambda_handler(event, context=None)

    index_html = mocked_s3.get_object(
        Bucket=bucket,
        Key='index.html',)['Body'].read().decode()

    link = '<a href="{}/{}/{}">{}</a>'.format(
        domain, folder, FEED_FILENAME, folder)
    assert link in index_html


def test_index_with_additional_feed(
        mocked_s3, uploaded_episodes, bucket, domain,
        folder, event,
        folder2, event2
):
    lambda_handler(event, context=None)

    mocked_s3.put_object(
        Bucket=bucket,
        Key='{}/talking-17.mp3'.format(folder2),
        Body='talky talky',)

    lambda_handler(event2, None)

    index_html = mocked_s3.get_object(
        Bucket=bucket,
        Key='index.html',)['Body'].read().decode()

    link = '<a href="{}/{}/{}">{}</a>'.format(
        domain, folder, FEED_FILENAME, folder)
    assert link in index_html

    link2 = '<a href="{}/{}/{}">{}</a>'.format(
        domain, quote_plus(folder2), FEED_FILENAME, escape(folder2))
    assert link2 in index_html

# test next
# - lambda test button
# - no folder
# -
