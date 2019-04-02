import pandas as pd
import numpy as np

#   INFO
#
#   ponoć wykorzystanie w Pandas funkcji agregującej - argument aggfunc jest dosyć nietypowe i warte ujęcia w sprawku


class PandasMovies:
    ''' wczytanie danych z plików '''
    def __init__(self):
        self._rated_movies = pd.read_csv("../dane/user_ratedmovies.dat.txt", sep='\t', dtype={"userID": int, "rating":np.float64}, nrows=100)
        self._movie_genres = pd.read_csv("../dane/movie_genres.dat.txt", sep='\t', dtype={"movieID": int})

        self._movie_genres_dummy = self._movie_genres.copy()
        self._movie_genres_dummy['dummy_column'] = 1

        joined = self._rated_movies.join(self._movie_genres, rsuffix='genres')
        self._tables = joined

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
    pm = PandasMovies()

    #zad 4
    print("ZAD 4")
    print(bezstratnosc(pm))

    #zad 5
    print("ZAD 5")
    print(dfToDict(pm.getAvg()))

    #zad 5 diagnostyczne
    print("ZAD 5 - DIAGNOSTYCZNE")
    for item in dfToDict(pm.rewriteRatingToGenreColumn()):
        print(item)

    #zad 6
    print("ZAD 6")
    print(dfToDict(pm.getPivotUser(78)))

    #zad 7
    print("ZAD 7")
    print(dfToDict(pm.getDifferenceWithAvgUser(78)))


