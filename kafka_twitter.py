from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaConsumer, KafkaProducer

ACCESS_TOKEN = 'XXXXXXX'
ACCESS_SECRET = 'XXXXX'
CONSUMER_KEY = 'XXXXX'
CONSUMER_SECRET = 'XXXXX'



kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092')
class StdOutListener(StreamListener):
    def data1(self, data):
        kafka_producer.send("messi", data.encode('utf-8')).get(timeout=10)
        print (data)
        return True
    def error1(self, status):
        print (status)



lobjectt= StdOutListener()
auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
stream = Stream(auth, lobjectt)
stream.filter(track=["messi"])