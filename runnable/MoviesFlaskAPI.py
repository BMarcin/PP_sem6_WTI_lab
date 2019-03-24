from flask import Flask, jsonify, abort, request, make_response
import json
from klasy.PandasMovies import PandasMovies


''' ZALADOWANIE FILMOW '''
pm = PandasMovies()
pivoted = pm.getPivotAllTable()


''' DEFINICJA APKI '''
app = Flask(__name__, static_url_path="")


''' DEFINICJE BLEDOW '''
@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)



''' WŁAŚCIWE ENDPOINTY '''
@app.route('/ratings', methods=['DELETE'])
def ratings():
    return jsonify({}), 200, {"Content-Type": "application/json"}



@app.route('/ratings', methods=["GET"])
def xx():
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





if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)