from cassandra.cluster import Cluster
from cassandra.query import dict_factory


class CassandraClient:
    def __init__(self, host, port=9042):
        self.keyspace = "user_ratings"
        self.table = "user_ratedmovies"

        self.cluster = Cluster([host], port=port)
        self.session = self.cluster.connect()

        self.create_keyspace()
        self.create_table()

        self.lastindex = 0

    def create_keyspace(self):
        self.session.execute("CREATE KEYSPACE IF NOT EXISTS "+self.keyspace+" WITH replication = { 'class': "
                                                                            "'SimpleStrategy', 'replication_factor': "
                                                                            "'1' }")

    def create_table(self):
        self.session.execute("CREATE TABLE IF NOT EXISTS "+self.keyspace+"."+self.table+" (wpisID int, userID int , movieID int, "
                                                                                        "rating float, date_day int, "
                                                                                        "date_month int, "
                                                                                        "date_year int, "
                                                                                        "date_hour int, date_minute "
                                                                                        "int, date_second int, PRIMARY "
                                                                                        "KEY(wpisID))")

    def push_data_table(self, wpisID, userID, movieID, rating, date_day, date_month, date_year, date_hour, date_minute, date_second):
        self.session.execute("INSERT INTO "+self.keyspace+"."+self.table+"(wpisID, userID, movieID, rating, date_day, "
                                                                         "date_month, date_year, date_hour, "
                                                                         "date_minute, date_second) VALUES (%(wpisID)s, %("
                                                                         "userID)s, %(movieID)s, %(rating)s, "
                                                                         "%(date_day)s, %(date_month)s, "
                                                                         "%(date_year)s, %(date_hour)s, "
                                                                         "%(date_minute)s, %(date_second)s)",
        {
            "wpisID": wpisID,
            'userID': userID,
            'movieID': movieID,
            'rating': rating,
            'date_day': date_day,
            'date_month': date_month,
            'date_year': date_year,
            'date_hour': date_hour,
            'date_minute': date_minute,
            'date_second': date_second
        })
        self.lastindex = len(self.get_data_table())

    def get_data_table(self):
        rows = self.session.execute("SELECT * FROM "+self.keyspace+"."+self.table+";")

        zwrot = []

        # moglo by nie byc hardcoded xD

        for row in rows:
            wiersz = {}
            wiersz["userID"] = row.userid
            wiersz["movieID"] = row.movieid
            wiersz["rating"] = row.rating
            wiersz["date_day"] = row.date_day
            wiersz["date_month"] = row.date_month
            wiersz["date_year"] = row.date_year
            wiersz["date_hour"] = row.date_hour
            wiersz["date_minute"] = row.date_minute
            wiersz["date_second"] = row.date_second

            zwrot.append(wiersz)

        return zwrot

    def clear_table(self):
        self.session.execute("TRUNCATE "+self.keyspace+"."+self.table+";")
        self.lastindex = 0

    def delete_table(self):
        self.session.execute("DROP TABLE "+self.keyspace+"."+self.table+";")
        self.lastindex = 0


if __name__ == "__main__":
    cc = CassandraClient('localhost', port=9043)

    # print(cc.get_data_table())

    # cc.push_data_table(1, 1, 2.0, 1, 1, 1, 1, 1, 1)

    # print(cc.get_data_table())

    # cc.clear_table()
    # print(cc.get_data_table())