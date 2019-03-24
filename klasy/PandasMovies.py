import pandas as pd
import numpy as np

#   INFO
#
#   ponoć wykorzystanie w Pandas funkcji agregującej - argument aggfunc jest dosyć nietypowe i warte ujęcia w sprawku


class PandasMovies:
    ''' wczytanie danych z plików '''
    def __init__(self):
        self._rated_movies = pd.read_csv("../dane/user_ratedmovies.dat.txt", sep='\t', dtype={"userID": int, "rating":np.float64})
        self._movie_genres = pd.read_csv("../dane/movie_genres.dat.txt", sep='\t', dtype={"movieID": int})

        self._movie_genres_dummy = self._movie_genres.copy()
        self._movie_genres_dummy['dummy_column'] = 1

        #joined = rated_movies.join(movie_genres, rsuffix='genres')
        #self._tables = joined

    ''' zrobienie Pivota na całą tabele '''
    def getPivotAllTable(self):
        pivoted = self._movie_genres_dummy.pivot_table(index=['movieID'], columns='genre', values='dummy_column',
                                                 fill_value=0).add_prefix("genre-")
        joined = pd.merge(self._rated_movies, pivoted, on="movieID").drop(["date_day", "date_minute", "date_month", "date_second", "date_year", "date_hour"], axis=1).astype(int)
        return joined

    ''' pobranie wszystkich gatunków na podstawie ID usera '''
    def getPivotUser(self, userID):
        joined = pd.merge(self._rated_movies[self._rated_movies.userID == userID], self._movie_genres, on="movieID")
        pivoted = joined.pivot_table(columns='genre', fill_value=0, aggfunc=np.mean, values="rating").add_prefix("genre-")

        return pivoted

    ''' pobranie czystego pivota '''
    def getAvg(self):
        joined = pd.merge(self._rated_movies, self._movie_genres, on="movieID")
        pivoted = joined.pivot_table(columns='genre', fill_value=0, aggfunc=np.mean, values="rating").add_prefix("genre-")

        return pivoted


if __name__ == '__main__':
    pm = PandasMovies()
    print(pm.getPivotUser(78))