from flask import Flask, jsonify, abort, request, make_response
import json
from klasy.PandasMovies import PandasMovies
from klasy.PandasMovies import dictToDF





''' ZALADOWANIE FILMOW '''
pm = PandasMovies(datasetrows=5000, useRedis=True, RedisHost="localhost", RedisPort=6379, RedisDB=0)





''' DEFINICJA APKI '''
app = Flask(__name__, static_url_path="")





''' DEFINICJE BLEDOW '''
@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)





''' WŁAŚCIWE ENDPOINTY '''
@app.route('/ratings', methods=['DELETE'])
def ratings():
    result = request.get_json()
    #pm.dropRecord(result["userID"], result["rating"], result["movieID"])
    pm.fullDrop()
    return jsonify({}), 200, {"Content-Type": "application/json"}



@app.route('/ratings', methods=["GET"])
def xx():
    pivoted = pm.getPivotAllTable()
    zwrot = []

    for index, row in pivoted.iterrows():
        if index < 100:
            zwrot.append(json.loads(row.to_json(orient='columns')))
        else:
            break

    return jsonify(zwrot), 200, {"Content-Type": "application/json"}



@app.route('/rating', methods=['POST'])
def rating():
    result = request.get_json()
    pm.appendRecord(result["userID"], result["rating"], result["movieID"])

    return jsonify(result), 200, {"Content-Type": "application/json"}



@app.route('/avg-genre-ratings/all-users', methods=["GET"])
def all_users():
    zwrot = []
    for index, row in pm.getAvg().iterrows():
        zwrot.append(json.loads(row.to_json(orient='columns')))

    return jsonify(zwrot[0]), 200, {"Content-Type": "application/json"}



@app.route('/avg-genre-ratings/<int:userID>', methods=["GET"])
def avg_user(userID):
    zwrot = []
    for index, row in pm.getPivotUser(userID).iterrows():
        zwrot.append(json.loads(row.to_json(orient='columns')))

    return jsonify(zwrot[0]), 200, {"Content-Type": "application/json"}


''' end point odnoszacy sie do czesci '''
#       "przy czym profil ten powinien być zbiorem
#        różnic między średnią oceną filmów danego gatunku a średnią oceną filmów danego
#        gatunku udzieloną przez danego użytkownika."
@app.route('/profile/<int:userID>', methods=['GET'])
def userprofile(userID):
    zwrot = []
    for index, row in pm.getDifferenceWithAvgUser(userID).iterrows():
        zwrot.append(json.loads(row.to_json(orient='columns')))

    return jsonify(zwrot[0]), 200, {"Content-Type": "application/json"}





if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)