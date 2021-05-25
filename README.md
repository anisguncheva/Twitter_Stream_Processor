# Twitter_Stream_Processor
Real-time data stream from Twitter, analysed with Apache Kafka.  
Steps to run the project: 
1. Create a developers account on twitter and get your own API keys to enter in credentials.py file 
2. Install and run apache kafka 
3. Change IP address in files wherever it says bootstrap_servers=['3.88.234.205:9092'] to your kafka server ip address
4. To check tweets - run tweets_producer.py and then tweets_consumer.py
5. To check trend - run trends_producer.py and trends_consumer.py 
