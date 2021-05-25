# import libraries
from json import loads
from datetime import datetime, timedelta
from kafka import KafkaConsumer, TopicPartition
from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt
from PIL import Image
import numpy as np
from os import path

# vars used or user menu
num_trends =0
selected_time=0

# word cloud set up
def plot_wordcloud(text):
    stopwords = set(STOPWORDS)
    stopwords.update(['https', 'http', 't', 'RT', 'co'])
    image_file = path.dirname(__file__)
    logomask = np.array(Image.open(path.join(image_file, 'twitter.jpg')))

    # Generate a word cloud image
    wordcloud = WordCloud(stopwords=stopwords,mask=logomask, background_color="white").generate(text)

    # Display the generated image:
    # the matplotlib way:
    plt.figure(figsize=(20, 10), facecolor='k')
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    plt.show()

# consumer that gets the trending topics based on user selected time and
# num of trends. Lastly displays them in a word cloud format
def check_topics(minutes_ago, trends_to_display):
    consumer = KafkaConsumer(
        bootstrap_servers=['3.88.234.205:9092'],
        auto_offset_reset='latest',
        max_poll_records=1000000,
        enable_auto_commit=True,
        group_id='my-group')
    partition = TopicPartition('trending_topics', 0)
    consumer.assign([partition])
    timestamp = int((datetime.now() - timedelta(minutes=minutes_ago)).timestamp() * 1000)
    offset = consumer.offsets_for_times({partition: timestamp})
    offset = offset[partition][0]
    consumer.seek(partition, offset)
    sentences = ""
    trends = {}
    now=datetime.utcnow()
    print(now)
    for message in consumer:
        topics = loads(message.value)
        i = 50
        for topic in topics:
            sentences += topic['name']
            sentences += ' '
            if topic['name'] in trends:
                trends[topic['name']] = trends[topic['name']]+i
            else:
                trends[topic['name']] = i
            i -= 1
        end = consumer.end_offsets([partition])[partition]
        current_position = (consumer.position(partition))
        if end <= current_position:
            break
    sorted_keys = sorted(trends, key=trends.get, reverse=True)
    j = 0
    for key in sorted_keys:
        j += 1
        print(str(j) + ') ' + key)
        if j >= trends_to_display:
            break
    plot_wordcloud(sentences)

# get user selected time
def get_time():
    answer = True
    while answer:
        print("""
                   Trending Topics: select time

                   1.10 minutes
                   2.30 minutes
                   3.60 minutes               
                   """)
        answer = input("Please select the time of topics to be shown \n")
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

# get user choice on num of trends to display
def get_num_of_trends():
    answer = True
    while answer:
        print("""
                      Trending Topics: 

                      1.Top 10 trending topics
                      2.Top 30 trending topics 
                      3.Top 50 trending topics              
                      """)
        answer = input("Please select the number of trending topics to be shown \n")
        if answer == "1":
            num_trends = 10
            answer = False

        elif answer == "2":
            num_trends = 30
            answer = False

        elif answer == "3":
            num_trends = 50
            answer = False

        else:
            print("\n Not Valid Choice Try again")

    return num_trends

if __name__ == '__main__':
    # run script and functions
    time_selected = get_time()
    num_trends = get_num_of_trends()
    check_topics(time_selected, num_trends)

