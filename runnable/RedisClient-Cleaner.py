from time import sleep
from klasy.RedisClient import RedisClient

x = 'aaa'

r = RedisClient('localhost', 6379, 0)

#while True:
r.wyczysc()

print("cleared")
    #sleep(3)

