# Repo zawierające zadanka na laby z WTI

## Materiały
http://pawel.misiorek.pracownik.put.poznan.pl/zajecia/wti/

## Laby 1
### Pliki zawierające rozwiązania
+ [docker/redis/docker-compose.yml](https://github.com/BMarcin/PP_sem6_WTI_lab/blob/master/docker/redis/docker-compose.yml)
### Info
+ uruchomienie Redis'a komendą: ```docker-compose up```

## Laby 2
### Pliki zawierające rozwiązania
+ [dane/user_ratedmovies.dat.txt](https://github.com/BMarcin/PP_sem6_WTI_lab/blob/master/dane/user_ratedmovies.dat.txt)
+ [klasy/RedisClient.py](https://github.com/BMarcin/PP_sem6_WTI_lab/blob/master/klasy/RedisClient.py)
+ [runnable/RedisClient-Cleaner.py](https://github.com/BMarcin/PP_sem6_WTI_lab/blob/master/runnable/RedisClient-Cleaner.py)
+ [runnable/RedisClient-Reader.py](https://github.com/BMarcin/PP_sem6_WTI_lab/blob/master/runnable/RedisClient-Reader.py)
+ [runnable/RedisClient-Writer.py](https://github.com/BMarcin/PP_sem6_WTI_lab/blob/master/runnable/RedisClient-Writer.py)
### Info
+ Pandas lubi wywalać błędy przy wczytywaniu pliku CSV, jeżeli jako argument podajemy plik z rozszerzeniem .dat. W celu rozwiązania problemu wystarczy zmienić rozszerzenie z .dat na np. .dat.txt

## Laby 3
### Pliki zawierające rozwiązania
+ [dane/movie_genres.dat.txt](https://github.com/BMarcin/PP_sem6_WTI_lab/blob/master/dane/movie_genres.dat.txt)
+ [klasy/PandasMovies.py](https://github.com/BMarcin/PP_sem6_WTI_lab/blob/master/klasy/PandasMovies.py)
+ [runnable/MoviesFlaskAPI.py](https://github.com/BMarcin/PP_sem6_WTI_lab/blob/master/runnable/MoviesFlaskAPI.py)
+ [runnable/MoviesFlaskAPITesterClient.py](https://github.com/BMarcin/PP_sem6_WTI_lab/blob/master/runnable/MoviesFlaskAPITesterClient.py)

### Info
+ Błąd odnośnie Pandas jak w Labach wyżej
+ Dodatkowo, ponoć uzycie funkcji agregującej w Pandas podczas pivot_table jest "ciekawą" funkcją wartą zaprezentowania w sprawku