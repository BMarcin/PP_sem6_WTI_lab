import pandas as pd
import numpy as np

#   INFO
#
#   ponoć wykorzystanie w Pandas funkcji agregującej - argument aggfunc jest dosyć nietypowe i warte ujęcia w sprawku


class PandasMovies:
    ''' wczytanie danych z plików '''
    def __init__(self):
        rated_movies = pd.read_csv("../dane/user_ratedmovies.dat.txt", sep='\t', dtype={"userID": int})
        movie_genres = pd.read_csv("../dane/movie_genres.dat.txt", sep='\t', dtype={"movieID": int})
        joined = rated_movies.join(movie_genres, rsuffix='genres')

        self._tables = joined

    ''' zrobienie Pivota na całą tabele '''
    def getPivotAllTable(self):
        pivoted = self._tables.pivot_table(index=['userID', 'movieID', "rating"], columns='genre', values='date_day',
                                     aggfunc=len, fill_value=0).add_prefix("genre-").reset_index()
        return pivoted.astype(object)

    ''' pobranie wszystkich gatunków na podstawie ID usera '''
    def getPivotUser(self, userID):
        pivoted = self._tables[self._tables.userID == userID].pivot_table(index=['userID', 'movieID', "rating"], columns='genre',
                                                values='date_day', aggfunc=len, fill_value=0).add_prefix("genre-").reset_index()
        return pivoted.astype(object)

    ''' pobranie czystego pivota '''
    def getClearPivot(self):
        pivoted = self._tables.pivot_table(columns='genre', values='rating', aggfunc=np.mean, fill_value=0).add_prefix("genre-")

        return pivoted.astype(object)