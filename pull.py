import argparse
import json
import time
from google.cloud import pubsub_v1
import os

def summarize(message):
    # [START parse_message]
    data = message.data.decode('utf-8')
    attributes = message.attributes
    bucket_id = attributes['bucketId']
    object_id = attributes['objectId']
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
        cmd = "spark-submit a.py "+str(f)
        print(cmd)
        # returned_value = os.system(cmd)
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
