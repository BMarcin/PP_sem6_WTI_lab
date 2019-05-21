import cherrypy
import json

from klasy.PandasMovies import PandasMovies





@cherrypy.expose
@cherrypy.tools.json_out()
class ratings(object):
    @cherrypy.tools.accept(media='application/json')
    def GET(self):
        pivoted = pm.getPivotAllTable()
        zwrot = []

        for index, row in pivoted.iterrows():
            if index < 100:
                zwrot.append(json.loads(row.to_json(orient='columns')))
            else:
                break

        return zwrot



    def DELETE(self):
        pm.fullDrop()
        return {}





@cherrypy.expose
@cherrypy.tools.json_out()
class rating(object):
    @cherrypy.tools.accept(media="application/json'")
    def POST(self):
        cl = cherrypy.request.headers['Content-Length']
        rawbody = cherrypy.request.body.read(int(cl))
        result = json.loads(rawbody)
        pm.appendRecord(result["userID"], result["rating"], result["movieID"])
        return result





@cherrypy.expose
@cherrypy.tools.json_out()
class avg_all(object):
    @cherrypy.tools.accept(media="application/json'")
    def GET(self, args):
        if args == 'all-users':
            zwrot = []
            for index, row in pm.getAvg().iterrows():
                zwrot.append(json.loads(row.to_json(orient='columns')))
            return zwrot[0]
        else:
            zwrot = []
            for index, row in pm.getPivotUser(int(args)).iterrows():
                zwrot.append(json.loads(row.to_json(orient='columns')))
            return zwrot[0]


@cherrypy.expose
@cherrypy.tools.json_out()
class profile(object):
    @cherrypy.tools.accept(media="application/json")
    def GET(self, arg):
        zwrot = []
        for index, row in pm.getDifferenceWithAvgUser(int(arg)).iterrows():
            zwrot.append(json.loads(row.to_json(orient="columns")))

        return zwrot





if __name__ == '__main__':
    pm = PandasMovies(datasetrows=2, useRedis=False, RedisHost="localhost", RedisPort=16786, RedisDB=0, useCassandra=True, CassandraPort=9043, CassandraHost='localhost')



    conf = {
        '/':{
            'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
            'tools.response_headers.on': True,
            'tools.response_headers.headers': [('Content-Type', 'application/json')],
        },
        'global': {
            'engine.autoreload.on': False
        }
    }



    cherrypy.config.update({'server.socket_port': 6161})
    cherrypy.tree.mount(ratings(), '/ratings', conf)
    cherrypy.tree.mount(rating(), "/rating", conf)
    cherrypy.tree.mount(avg_all(), "/avg-genre-ratings", conf)
    cherrypy.tree.mount(profile(), "/profile", conf)

    cherrypy.engine.start()
    cherrypy.engine.block()