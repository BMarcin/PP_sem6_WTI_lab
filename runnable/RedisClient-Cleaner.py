from time import sleep
from klasy.RedisClient import RedisClient

x = 'aaa'

r = RedisClient('localhost', 16786, 0)

#while True:
r.wyczysc()

print("cleared")
    #sleep(3)

