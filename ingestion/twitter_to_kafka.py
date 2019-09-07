import json
from kafka import KafkaProducer, KafkaClient
import tweepy
import yaml

# Note: Some of the imports are external python libraries. They are installed on the current machine.
# If you are running multinode cluster, you have to make sure that these libraries
# and currect version of Python is installed on all the worker nodes.


class TweeterStreamListener(tweepy.StreamListener):
    """ A class to read the twiiter stream and push it to Kafka"""

    def __init__(self, api, kafka_host='localhost:9092', stream_config={}):
        self.api = api
        self.stream_config = stream_config

        super(tweepy.StreamListener, self).__init__()
        self.producer = KafkaProducer(bootstrap_servers=kafka_host)

        # Add Kafka topics
        topic = self.stream_config.get('kafka_topic')
        if topic:
            client = KafkaClient(kafka_host)
            client.add_topic(topic)

    def on_status(self, status):
        """ This method is called whenever new data arrives from live stream.
        We asynchronously push this data to kafka queue"""
        msg = status.text
        id_str = status.id_str
        created_at = status.created_at
        messages = { 'id_str': id_str, 'msg': msg, 'created_at': created_at }

        try:
            self.producer.send(self.stream_config, json.dumps(messages).encode('utf-8'))
        except Exception as e:
            print(e)
            return False
        return True

    def on_error(self, status_code):
        print("Error received in kafka producer")
        return True  # Don't kill the stream

    def on_timeout(self):
        return True  # Don't kill the stream


if __name__ == '__main__':

    # Read the credententials from 'twitter.txt' file
    config = yaml.safe_load(open('./twitter_config.yml', 'r'))
    consumer_key = config['consumerKey']
    consumer_secret = config['consumerSecret']
    access_key = config['accessToken']
    access_secret = config['accessTokenSecret']
    kafka_host = config['kafkaHost']
    stream_config = config['stream_config']

    # Create Auth object
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth)

    # Custom Filter rules pull all traffic for those filters in real time.
    #stream.filter(track = ['love', 'hate'], languages = ['en'])
    for config in stream_config:
        # Create stream and bind the listener to it
        stream = tweepy.Stream(auth, listener=TweeterStreamListener(
            api, kafka_host, stream_config=config))
        print(f'Track by keywords: {config.get("twitter_keywords")}')
        stream.filter(track=config.get('twitter_keywords'))
