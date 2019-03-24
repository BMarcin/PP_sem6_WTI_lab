from klasy.RedisClient import RedisClient
import json
from time import sleep

x = 'aaa'

r = RedisClient('localhost', 6379, 0)



while True:
    pobrane = r.lrange("kolejka", 0, -1)
    r.ltrim("kolejka", len(pobrane), -1)

    for el in pobrane:
        print(json.loads(el))
        sleep(0.01)
