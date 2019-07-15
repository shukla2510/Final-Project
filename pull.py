import argparse
import json
import time
from google.cloud import pubsub_v1
import os

def summarize(message):
    # [START parse_message]
    data = message.data.decode('utf-8')
    attributes = message.attributes

    event_type = attributes['eventType']
    bucket_id = attributes['bucketId']
    object_id = attributes['objectId']
    generation = attributes['objectGeneration']
    description = (
        '\tEvent type: {event_type}\n'
        '\tBucket ID: {bucket_id}\n'
        '\tObject ID: {object_id}\n'
        '\tGeneration: {generation}\n').format(
            event_type=event_type,
            bucket_id=bucket_id,
            object_id=object_id,
            generation=generation)

    if 'overwroteGeneration' in attributes:
        description += '\tOverwrote generation: %s\n' % (
            attributes['overwroteGeneration'])
    if 'overwrittenByGeneration' in attributes:
        description += '\tOverwritten by generation: %s\n' % (
            attributes['overwrittenByGeneration'])

    payload_format = attributes['payloadFormat']
    if payload_format == 'JSON_API_V1':
        object_metadata = json.loads(data)
        size = object_metadata['size']
        content_type = object_metadata['contentType']
        metageneration = object_metadata['metageneration']
        description += (
            '\tContent type: {content_type}\n'
            '\tSize: {object_size}\n'
            '\tMetageneration: {metageneration}\n').format(
                content_type=content_type,
                object_size=size,
                metageneration=metageneration)
    return bucket_id, object_id
    # [END parse_message]

def poll_notifications(project, subscription_name):
    """Polls a Cloud Pub/Sub subscription for new GCS events for display."""
    # [BEGIN poll_notifications]
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        project, subscription_name)

    def callback(message):
        bucket_id, object_id = summarize(message)
        print('Received message:\n{}'.format(bucket_id))
        print('Received message:\n{}'.format(object_id))
        f = 'gs://'+str(bucket_id)+'/'+str(object_id)
        cmd = "spark-submit a.py"
        print(cmd)
        returned_value = os.system(cmd)
        print(f)
        message.ack()

    subscriber.subscribe(subscription_path, callback=callback)

    # The subscriber is non-blocking, so we must keep the main thread from
    # exiting to allow it to process messages in the background.
    print('Listening for messages on {}'.format(subscription_path))
    while True:
        time.sleep(1)
    # [END poll_notifications]


if __name__ == '__main__':
    # while True:

        poll_notifications('hadooplearning-236009', 'my-subs')
