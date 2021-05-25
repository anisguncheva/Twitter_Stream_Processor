# import libraries
import re
from json import loads
from datetime import datetime, timedelta
from dateutil import parser
from kafka import KafkaConsumer, TopicPartition
from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt

from PIL import Image
import numpy as np
from os import path

# variables used for the menu
time_selected = 0
num_tweets = 0

# wordcloud set up function
def plot_wordcloud(text):
    stopwords = set(STOPWORDS)
    stopwords.update(['https', 'http', 't', 'RT', 'co'])
    image_file = path.dirname(__file__)
    logomask = np.array(Image.open(path.join(image_file, 'twitter.jpg')))

# Generate a word cloud image
    wordcloud = WordCloud(stopwords=stopwords,mask=logomask, background_color="white",max_font_size=1000).generate(text)

    # Display the generated image:
    # the matplotlib way:
    plt.figure(figsize=(20, 10), facecolor='k')
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    plt.show()

# func to retrieve tweets from topic and process which are trending based on a score. Lastly plot in word cloud format
def check_tweets(minutes_ago, tweet_to_display):
    consumer = KafkaConsumer(
        bootstrap_servers=['3.88.234.205:9092'],
        auto_offset_reset='latest',
        max_poll_records=1000000,
        enable_auto_commit=True,
        group_id='my-group')
    partition = TopicPartition('Tweets', 0)
    consumer.assign([partition])
    timestamp = int((datetime.now() - timedelta(minutes=minutes_ago)).timestamp() * 1000)
    offset = consumer.offsets_for_times({partition: timestamp})
    offset = offset[partition][0]
    consumer.seek(partition, offset)
    sentences = ""

    tweets = []
    now = datetime.utcnow()
    print(now)
    print("Results coming ...")
    for message in consumer:
        message = loads(loads(message.value))
        dt = parser.parse(message['created_at'])
        if 'retweeted_status' in message:
            rt = message['retweeted_status']
            score = int(3 * rt['retweet_count'] + rt['favorite_count'] + rt['reply_count'] + rt['quote_count'])
            tweet = {'text': rt['text'], 'by': rt['user']['name'], 'score': score}
            tweets.append(tweet)
            sentences += re.sub(r'^https?:\/\/.*[\r\n]*', '', rt['text'], flags=re.MULTILINE)
            sentences += ' '
        else:
            sentences += re.sub(r'^https?:\/\/.*[\r\n]*', '', message['text'], flags=re.MULTILINE)
            sentences += ' '
        if (now.hour == dt.hour) and (now.minute == dt.minute):
            break
    tweets.sort(key=lambda x: x['score'], reverse=True)
    i = 0
    for tweet in tweets:
        i += 1
        print(str(i) + ") " + tweet['text'] + '\n' + 'by ' + tweet['by'] + '\nscore: ' + str(tweet['score']))
        print()
        if i >= tweet_to_display:
            break
    plot_wordcloud(sentences)

# get time from user
def get_time():
    answer = True
    while answer:
        print("""
                   Trending Tweets: select time

                   1.10 minutes
                   2.30 minutes
                   3.60 minutes               
                   """)
        answer = input("Please select the time of tweets to be shown \n")
        if answer == "1":
            time_selected = 10
            answer = False

        elif answer == "2":
            time_selected = 30
            answer = False


        elif answer == "3":
            time_selected = 60
            answer = False

        else:
            print("\n Not Valid Choice Try again")

    return time_selected

# get num of tweets from user
def get_num_of_tweets():
    answer = True
    while answer:
        print("""
                      Trending Tweets:

                      1.Top 10 tweets
                      2.Top 50 tweets
                      3.Top 100 tweets              
                      """)
        answer = input("Please select the number of tweets to be shown \n")
        if answer == "1":
            num_tweets = 10
            answer = False

        elif answer == "2":
            num_tweets = 50
            answer = False

        elif answer == "3":
            num_tweets = 100
            answer = False

        else:
            print("\n Not Valid Choice Try again")

    return num_tweets


if __name__ == '__main__':
    # run the script and functions
     time_selected = get_time()
     num_tweets = get_num_of_tweets()
     check_tweets(time_selected,num_tweets)

