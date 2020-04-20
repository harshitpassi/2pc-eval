from bottle import Bottle
import bottle_mongo


def get_bottle_app_with_mongo(uri, db_name):
    app = Bottle()
    mongo_plugin = bottle_mongo.MongoPlugin(uri=uri, db=db_name)
    app.install(mongo_plugin)
    return app
