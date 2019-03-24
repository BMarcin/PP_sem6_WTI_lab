# Repo zawierające zadanka na laby z WTI

## Materiały
http://pawel.misiorek.pracownik.put.poznan.pl/zajecia/wti/

## Laby 1
### Pliki zawierające rozwiązania
+ docker/redis/docker-compose.yml

## Laby 2
### Pliki zawierające rozwiązania
+ dane/user_ratedmovies.dat.txt
+ klasy/RedisClient.py
+ runnable/RedisClient-Cleaner.py
+ runnable/RedisClient-Reader.py
+ runnable/RedisClient-Writer.py
### Info
Pandas lubi wywalać błędy przy wczytywaniu pliku CSV, jeżeli jako argument podajemy plik z rozszerzeniem .dat. W celu rozwiązania problemu wystarczy zmienić rozszerzenie z .dat na np. .dat.txt

## Laby 3
### Pliki zawierające rozwiązania
+ dane/movie_genres.dat.txt
+ klasy/PandasMovies.py
+ runnable/MoviesFlaskAPI.py
+ runnable/MoviesFlaskAPITesterClient.py

### Info
Błąd odnośnie Pandas jak w Labach wyżej
Dodatkowo, ponoć uzycie funkcji agregującej w Pandas podczas pivot_table jest "ciekawą" funkcją wartą zaprezentowania w sprawku