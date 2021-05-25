# import libraries
from json import dumps
import tweepy
from kafka import KafkaProducer
from tweepy.streaming import StreamListener
from credentials import consumer_key, consumer_secret, access_token, access_secret


# get authorised using the credentials file with the twitter api keys
def get_authorised():
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    return tweepy.API(auth)

# listener class that sets up the producer topic as Tweets
class MyStreamListener(StreamListener):
    def __init__(self, producer):
        super().__init__()
        self.producer = producer

    def on_data(self, data):
        self.producer.send("Tweets", data)
        print(data)
        return True

    def on_error(self, status_code):
        if status_code == 420:
            return False

# func to get the tweets using tweepy stream and kafka producer
def get_tweets(api):
    myStreamListener = MyStreamListener(
        KafkaProducer(bootstrap_servers=['3.88.234.205:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))) # change the bootstrap sever ip to your own kafka server
    myStream = tweepy.streaming.Stream(auth=api.auth, listener=myStreamListener)

    try:
        print('Start tweet streaming.')
        myStream.sample(languages=['en'])
    except KeyboardInterrupt:
        print("Stopped.")
    finally:
        print('Done.')
        myStream.disconnect()


if __name__ == '__main__':
    # run the script and functions
    api= get_authorised()
    get_tweets(api)


