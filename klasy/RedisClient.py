import redis

class RedisClient:
    def __init__(self, host, port, db):
        self._redis = redis.Redis(host=host, port=port, db=db)

    def wyczysc(self):
        return self._redis.flushdb()

    def rpush(self, kolejka, _json):
        return self._redis.rpush(kolejka, _json)

    def lrange(self, kolejka, _from, _to):
        return self._redis.lrange(kolejka, _from, _to)

    def ltrim(self, kolejka, _from, _to):
        return self._redis.ltrim(kolejka, _from, _to)

    def wyczysc_kolejke(self, nazwakolejki):
        pobrane = self._redis.lrange(nazwakolejki, 0, -1)
        self._redis.ltrim(nazwakolejki, len(pobrane), -1)