import argparse
import json
import time
import ConfigParser
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


def poll_notifications(project, subscription_name, raw_bucket, refined_bucket, job_path):
    """Polls a Cloud Pub/Sub subscription for new GCS events for display."""
    # [BEGIN poll_notifications]
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        project, subscription_name)

    def callback(message):
        bucket_id, object_id = summarize(message)
        print('Received message:\n{}'.format(bucket_id))
        print('Received message:\n{}'.format(object_id))
        file_path = 'gs://{}/{}'.format(str(bucket_id), str(object_id))
        if 'mom' in object_id.lower():
            cmd = "spark-submit {} {} {} {}".format(mom_job_path, str(file_path), raw_bucket, refined_bucket)
            print(cmd)
            returned_value = os.system(cmd)
            print('returned_value : ', returned_value)
        else if 'program' in object_id.lower():
            cmd = "spark-submit {} {} {} {}".format(program_job_path, str(file_path), raw_bucket, refined_bucket)
            print(cmd)
            returned_value = os.system(cmd)
            print('returned_value : ', returned_value)    
        else:
            cmd = "spark-submit {} {} {} {}".format(job_path, str(file_path), raw_bucket, refined_bucket)
            print(cmd)
            returned_value = os.system(cmd)
            print('returned_value : ', returned_value)

        message.ack()

    subscriber.subscribe(subscription_path, callback=callback)

    # The subscriber is non-blocking, so we must keep the main thread from
    # exiting to allow it to process messages in the background.
    print('Listening for messages on {}'.format(subscription_path))
    while True:
        time.sleep(1)
    # [END poll_notifications]


if __name__ == '__main__':
    config = ConfigParser.ConfigParser()
    config.read('/var/.project/parameters.ini')
    stage = config.get('SECTION', 'stage').upper()
    project_id = config.get(stage, 'PROJECT_ID')
    raw_bucket = config.get(stage, 'RAW_BUCKET')
    refined_bucket = config.get(stage, 'REFINED_BUCKET')
    job_path = config.get(stage, 'JOB_PATH')
    mom_job_path = config.get(stage, 'MOM_JOB_PATH')
    topic_name = config.get(stage, 'LANDING_ZONE_TOPIC_NAME')
    subscriber_name = config.get(stage, 'LANDING_ZONE_SUBSCRIBER_NAME')
    # NUM_MESSAGES=int(config.get(stage,'MESSAGE_LIMIT')

    poll_notifications(project_id, subscriber_name, raw_bucket, refined_bucket, job_path)
