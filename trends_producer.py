# import libraries
import time
from json import dumps
import tweepy
from kafka import KafkaProducer
from credentials import consumer_key, consumer_secret, access_token, access_secret


# get authorised using the credentials file with the twitter api keys
def get_authorised():
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    return tweepy.API(auth)

# func that sets up the kafka producer with EC2 IP address
# it uses api.trends to extract the top 50 trends in the world, (1) arg after trends_place is the WOID
# send the data to the topic-  trending tweets
# it does it continuously
def get_trends(api):
    producer = KafkaProducer(bootstrap_servers=['3.88.234.205:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
    trending_topics = api.trends_place(1)[0]['trends']
    print('Start trending topics streaming')
    while True:
        try:
            i = 0
            for topic in trending_topics:
                i += 1
                print(str(i) + ") " + topic['name'])
            print()
            producer.send("trending_topics", trending_topics)
            time.sleep(60)
        except KeyboardInterrupt:
            print("Stopped.")
            break
    print('Done.')


if __name__ == '__main__':
    # Authentication to the Twitter API and run
    api = get_authorised()
    get_trends(api)