from write_rss_feed import FEED_FILENAME, lambda_handler


def test_rss_feed_written(
        mocked_s3, uploaded_episodes, bucket, folder, domain, event):
    lambda_handler(event, None)

    feed_path = '{}/feed.xml'.format(folder)
    feed_xml = mocked_s3.get_object(
        Bucket=bucket,
        Key=feed_path)['Body'].read()
    mp3s = [
        e['Key']
        for e in mocked_s3.list_objects_v2(Bucket=bucket)['Contents']
        if e['Key'].endswith('.mp3')
    ]
    for mp3 in mp3s:
        enclosure = '<enclosure url="{}/{}"'.format(domain, mp3)
        assert enclosure in feed_xml


def test_index_with_first_feed(
        mocked_s3, uploaded_episodes, bucket, folder, domain, event):
    lambda_handler(event, None)

    index_html = mocked_s3.get_object(
        Bucket=bucket,
        Key='index.html',)['Body'].read()

    link = '<a href="{}/{}/{}">{}</a>'.format(
        domain, folder, FEED_FILENAME, folder)
    assert link in index_html


def test_index_with_existing_feed(
        mocked_s3, uploaded_episodes, bucket, domain,
        folder, event,
        folder2, event2
):
    lambda_handler(event, None)

    mocked_s3.put_object(
        Bucket=bucket,
        Key='{}/talking-17.mp3'.format(folder2),
        Body='talky talky',)

    lambda_handler(event2, None)

    index_html = mocked_s3.get_object(
        Bucket=bucket,
        Key='index.html',)['Body'].read()

    link = '<a href="{}/{}/{}">{}</a>'.format(
        domain, folder, FEED_FILENAME, folder)
    assert link in index_html

    link2 = '<a href="{}/{}/{}">{}</a>'.format(
        domain, folder2, FEED_FILENAME, folder2)
    assert link2 in index_html
