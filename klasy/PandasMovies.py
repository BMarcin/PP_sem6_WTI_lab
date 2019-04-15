import pandas as pd
import numpy as np
from klasy.RedisClient import RedisClient
import json
from datetime import datetime

#   INFO
#
#   ponoć wykorzystanie w Pandas funkcji agregującej - argument aggfunc jest dosyć nietypowe i warte ujęcia w sprawku






class PandasMovies:
    ''' wczytanie danych z plików '''
    def __init__(self, datasetrows = None, useRedis = False, RedisHost = None, RedisPort = None, RedisDB = None):
        self._useRedis = useRedis

        # do redisa wrzucamy jedynie user_ratedmovies.dat.txt

        if useRedis == True:
            self._RClient = RedisClient(RedisHost, RedisPort, RedisDB)
            ''' detekcja, czy Redis jest pusty'''

            _rated_movies = [json.loads(el) for el in self._RClient.lrange("_rated_movies", 0, -1)]

            ''' podjecie decyzji skad wczytac dane '''
            if(len(_rated_movies) == 0):
                ''' nic nie bylo w redisie, wiec trzeba zaladowac dane z pliku'''
                print("Redis is empty")
                self.loadDatasetFromFiles(datasetrows)
                self.pushAllDataToRedis()
            else:
                ''' cos jest w redisie, wiec korzystamy z redisa '''
                self._rated_movies = dictToDF(_rated_movies)
        else:
            self.loadDatasetFromFiles(datasetrows)



        # dalsza czesc wczytywania danych
        # movie genres nie siedzi w redisie
        self._movie_genres = pd.read_csv("../dane/movie_genres.dat.txt", sep='\t', dtype={"movieID": int})
        self._movie_genres_dummy = self._movie_genres.copy()
        self._movie_genres_dummy['dummy_column'] = 1

        joined = self._rated_movies.join(self._movie_genres, rsuffix='genres')
        self._tables = joined

        print("Data loaded")



    def loadDatasetFromFiles(self, datasetrows):
        if datasetrows == None:
            self._rated_movies = pd.read_csv("../dane/user_ratedmovies.dat.txt", sep='\t', dtype={"userID": int, "rating": np.float64})
        else:
            self._rated_movies = pd.read_csv("../dane/user_ratedmovies.dat.txt", sep='\t', dtype={"userID": int, "rating": np.float64}, nrows=datasetrows)



    def pushAllDataToRedis(self):
        for index, row in self._rated_movies.iterrows():
            self._RClient.rpush("_rated_movies", row.to_json(orient='columns'))



    ''' pobranie czystego joina '''
    def getJoined(self):
        return self._tables



    ''' zrobienie Pivota na całą tabele '''
    def getPivotAllTable(self):
        self._pivoted = self._movie_genres_dummy.pivot_table(index=['movieID'], columns='genre', values='dummy_column',
                                                 fill_value=0).add_prefix("genre-")
        self._joined = pd.merge(self._rated_movies, self._pivoted, on="movieID").drop(["date_day", "date_minute", "date_month", "date_second", "date_year", "date_hour"], axis=1).astype(int)
        return self._joined



    ''' pobranie wszystkich gatunków na podstawie ID usera '''
    def getPivotUser(self, userID):
        self._joined = pd.merge(self._rated_movies[self._rated_movies.userID == userID], self._movie_genres, on="movieID")
        self._pivoted = self._joined.pivot_table(columns='genre', fill_value=0, aggfunc=np.mean, values="rating", dropna=False).add_prefix("genre-")

        return self._pivoted



    ''' pobranie czystego pivota '''
    def getAvg(self):
        self._joined = pd.merge(self._rated_movies, self._movie_genres, on="movieID")
        self._pivoted = self._joined.pivot_table(columns='genre', fill_value=0, aggfunc=np.mean, values="rating").add_prefix("genre-")

        return self._pivoted



    ''' wektory roznic dla kazdego gatunku na podstawie srednich ocen filmow wystawionych przez usera '''
    def getDifferenceWithAvgUser(self, userID):
        return self.getAvg().subtract(self.getPivotUser(userID)).fillna(0)



    ''' przepisanie ratingu pod kolumne z konkretnym gatunkiem '''
    def rewriteRatingToGenreColumn(self):
        self._pivoted = self._tables.pivot_table(index=['movieID'], columns='genre', values='rating',
                                                             fill_value=0).add_prefix("genre-")
        self._joined = pd.merge(self._rated_movies, self._pivoted, on="movieID").drop(
            ["date_day", "date_minute", "date_month", "date_second", "date_year", "date_hour"], axis=1).astype(int)
        return self._joined



    ''' dodawanie wpisu '''
    def appendRecord(self, userID, rating, movieID):
        #
        #   rozkminioine na chlopski rozum
        #   najprosciej dodac wpis a'la user_ratedmovies.dat.txt
        #   reszta bedzie dostosowana do tego
        #
        dic = {}
        dic["userID"] = userID
        dic["movieID"] = movieID
        dic["rating"] = rating
        dic["date_day"] = datetime.today().strftime("%d")
        dic["date_month"] = datetime.today().strftime("%m")
        dic["date_year"] = datetime.today().strftime("%y")
        dic["date_hour"] = datetime.today().strftime("%H")
        dic["date_minute"] = datetime.today().strftime("%M")
        dic["date_second"] = datetime.today().strftime("%S")

        df = pd.DataFrame(dic, index=[0])

        self._rated_movies = self._rated_movies.append(df, sort=True)
        self.reloadRatedMovies()

        if self._useRedis == True:
            self._RClient.rpush("_rated_movies", json.dumps(dic))



    ''' nie używana, ale moze się kiedyś przyda '''
    def dropRecord(self, userID, rating, movieID):
        # dropuje konkretny wpis
        self._rated_movies = self._rated_movies.drop(self._rated_movies[(self._rated_movies.userID == userID) & (self._rated_movies.rating == rating) & (self._rated_movies.movieID == movieID)].index)
        self.reloadRatedMovies()



    ''' pełen drop '''
    def fullDrop(self):
        self._rated_movies = self._rated_movies.iloc[0:0]
        self.reloadRatedMovies()

        if self._useRedis == True:
            self._RClient.wyczysc_kolejke("_rated_movies")



    ''' funkcja reloadowania, po wiekszych zmianach '''
    def reloadRatedMovies(self):
        joined = self._rated_movies.join(self._movie_genres, rsuffix='genres')
        self._tables = joined





''' Data frame na slownik '''
def dfToDict(df):
    return df.to_dict(orient='records')



''' Slownik na Data Frame '''
def dictToDF(dict):
    df = pd.DataFrame.from_dict(dict)
    return df.sort_index(axis=1)



''' sprawdzenie bezstratnosci '''
def bezstratnosc(df):
    xx = df.getJoined().sort_index(axis=1)
    xx2 = dictToDF(dfToDict(df.getJoined()))
    return (xx == xx2).all()




if __name__ == '__main__':
    pm = PandasMovies(datasetrows=2, useRedis=True, RedisHost="localhost", RedisPort=6379, RedisDB=0)
    #pm = PandasMovies()
    print(pm.getAvg())
    pm.appendRecord(78, 5, 3)
    #pm.fullDrop()
    print(pm.getAvg())



    #zad 4 lab4
    #print("ZAD 4")
    #print(bezstratnosc(pm))

    #zad 5 lab4
    #print("ZAD 5")
    #print(dfToDict(pm.getAvg()))

    #zad 5 diagnostyczne lab4
    #print("ZAD 5 - DIAGNOSTYCZNE")
    #for item in dfToDict(pm.rewriteRatingToGenreColumn()):
    #    print(item)

    #zad 6 lab4
    #print("ZAD 6")
    #print(dfToDict(pm.getPivotUser(78)))

    #zad 7 lab4
    #print("ZAD 7")
    #print(dfToDict(pm.getDifferenceWithAvgUser(78)))


