import redis
import json

class RecommendationStore(object):
    def __init__(self, host, port, db):
        self.host = host
        self.port = port
        self.db = db
        self.MODEL_PREFIX = 'br::uni7::recommendation::product::{0}'
        self.connection = redis.Redis(host=self.host, port=self.port, db=self.db)

    def save_recommendations(self, key, recommendations):
        self.connection.set(self.MODEL_PREFIX.format(key), recommendations)