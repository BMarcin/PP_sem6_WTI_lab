import time

import requests

class MovieFlaskAPITesterClient:
    def __init__(self, serviceAddress):
        self._serviceAddress = serviceAddress
        self._post_json = {
            "genre-Action": 0,
            "genre-Adventure": 0,
            "genre-Animation": 0,
            "genre-Children": 0,
            "genre-Comedy": 1,
            "genre-Crime": 0,
            "genre-Documentary": 0,
            "genre-Drama": 0,
            "genre-Fantasy": 0,
            "genre-Film-Noir": 0,
            "genre-Horror": 0,
            "genre-IMAX": 0,
            "genre-Musical": 0,
            "genre-Mystery": 0,
            "genre-Romance": 1,
            "genre-Sci-Fi": 0,
            "genre-Short": 0,
            "genre-Thriller": 0,
            "genre-War": 0,
            "genre-Western": 0,
            "movieID": 4,
            "rating": 1,
            "userID": 78
        }

    def testPostRatings(self):
        mypost = requests.post(self._serviceAddress + '/rating', json=self._post_json)
        print("POST /rating")
        print(mypost.text)

    def testGetRatings(self):
        myget = requests.get(self._serviceAddress + '/ratings')
        print("GET /ratings")
        print(myget.text)

    def testDeleteRatings(self):
        mydelete = requests.delete(self._serviceAddress + '/ratings', json=self._post_json)
        print("DELETE /ratings")
        print(mydelete.text)

    def testGetAvgGenreRatingsAllUsers(self):
        get = requests.get(self._serviceAddress + '/avg-genre-ratings/all-users')
        print("GET /avg-genre-ratings/all-users")
        print(get.text)

    def testGetAvgGenreRatins(self, userID):
        get2 = requests.get(self._serviceAddress + '/avg-genre-ratings/' + str(userID))
        print("GET /avg-genre-ratings/"+str(userID))
        print(get2.text)





if __name__ == '__main__':
    client = MovieFlaskAPITesterClient("http://localhost:6161")
    client.testPostRatings()
    #client.testGetRatings()
    #time.sleep(5)
    #client.testDeleteRatings()
    #client.testGetAvgGenreRatingsAllUsers()
    #client.testGetAvgGenreRatins(78)

    ##ZADANIE 4 - brak bezstanowość
    #client.testPostRatings()
    #client.testGetAvgGenreRatingsAllUsers()
    #print("ZRESTARTUJ API")
    #input("ZRESTARTUJ API")
    #client.testGetAvgGenreRatingsAllUsers()




