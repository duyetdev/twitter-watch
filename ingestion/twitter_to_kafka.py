import json
from kafka import SimpleProducer, KafkaClient
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
        # client = KafkaClient(kafka_host)
        # self.producer = SimpleProducer(client,
        #                                batch_send_every_n=1000,
        #                                batch_send_every_t=10)

    def on_status(self, status):
        """ This method is called whenever new data arrives from live stream.
        We asynchronously push this data to kafka queue"""
        msg = status.text.encode('utf-8')
        id_str = status.id_str
        created_at = status.created_at
        print(id_str, created_at, msg)

        # try:
        #     self.producer.send_messages(b'twitterstream', msg)
        # except Exception as e:
        #     print(e)
        #     return False
        # return True

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

    # Create stream and bind the listener to it
    stream = tweepy.Stream(auth, listener=TweeterStreamListener(
        api, kafka_host, stream_config))

    # Custom Filter rules pull all traffic for those filters in real time.
    #stream.filter(track = ['love', 'hate'], languages = ['en'])
    tracks = [config['twitter_keywords'] for config in stream_config]
    tracks = [item for sublist in tracks for item in sublist]

    print(f'Track by keywords: {tracks}')
    stream.filter(track=tracks)
