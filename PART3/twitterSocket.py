import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json

consumer_key='oS0Re7nXXKam0OBbbakyvFjY0'
consumer_secret='7hfFuDicBkwiByEDBKnEYATLpJuDVNvfVehMdXSbjLaGgqpCDe'
access_token ='1466794503004692483-4Z7pOXjTXIK7mHnfUQLZ9I4SkwaLWI'
access_secret='SUmdUWlhJ30oBQX5gpohFUIpaWdUPVm19ezS2FHR1vQdo'

class TweetsListener(StreamListener):
  def __init__(self, csocket):
      self.client_socket = csocket
        
  def on_data(self, data):
      try:
          msg = json.loads( data )
          print(msg['text'].encode('utf-8'))
          self.client_socket.send( msg['text'].encode('utf-8') )
          return True
      except BaseException as e:
          print("Error on_data: %s" % str(e))
      return True

  def on_error(self, status):
      print(status)
      return True

def sendData(c_socket):
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_secret)
  twitter_stream = Stream(auth, TweetsListener(c_socket))
  twitter_stream.filter(track=['modi'])

if __name__ == '__main__':
    s = socket.socket()
    host = "127.0.0.1"
    port = 2222
    s.bind((host,port))
    print("Listening on port : %s" % str(port))
    s.listen(5)
    c, addr = s.accept()
    print("Received data from : " + str(addr))  

sendData(c)