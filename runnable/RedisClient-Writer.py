from time import sleep
from klasy.RedisClient import RedisClient
import pandas as pd
x = 'aaa'

r = RedisClient('localhost', 6379, 0)

df = pd.read_csv("../dane/user_ratedmovies.dat.txt", nrows=100, sep='\t', dtype={"userID": int})
df = df.astype(object)

while True:
    for index, row in df.iterrows():
        r.rpush("kolejka", row.to_json(orient='columns'))
        sleep(1)
        print(row.to_json())
