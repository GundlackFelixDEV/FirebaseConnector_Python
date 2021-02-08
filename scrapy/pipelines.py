from scrapy.exporters import PythonItemExporter
from datetime import datetime
import os
from ..FirestoreConnector import FirestoreConnector


class FirebaseConnector(object):
    @classmethod
    def get_settings(cls, crawler, settings):
        db_settings = crawler.settings.getdict(settings)
        if not db_settings:
            raise f"Settings [{settings}] missings!"

        coll = db_settings['collection']
        cred = db_settings['cred']
        proj = db_settings['project']

        return coll, cred, proj

    def __init__(self, credentials, project, collection):
        self.collection = collection
        self.credentials = credentials
        self.project = project
        self.db = FirestoreConnector(self.project, self.credentials)


class FirebaseStatusPipeline(FirebaseConnector):
    @classmethod
    def from_crawler(cls, crawler):
        SETTINGS = "FIREBASE_STATUS_SETTINGS"
        coll, cred, proj = FirebaseConnector.get_settings(crawler, SETTINGS)

        db_settings = crawler.settings.getdict(SETTINGS)
        document = db_settings['document']
        assert len(document) > 0, "Missing 'document' to save status!"
        return cls(credentials=cred, project=proj, collection=coll, document=document)

    def __init__(self, credentials, project, collection, document):
        FirebaseConnector.__init__(self, credentials, project, collection)
        self.document = document
        self.n_items = 0

    def update_status(self, query, status):
        data = {
            "_id": self.document,
            "session": {query: status}
        }
        self.db.update_collection(self.collection, data)

    def open_spider(self, spider):
        query_status = {
            'status': "crawling",
            'result': None
        }
        self.update_status(spider.query, query_status)

    def close_spider(self, spider):
        query_status = {
            'status': "ready",
            'result': self.n_items
        }
        self.update_status(spider.query, query_status)

    def process_item(self, item, spider):
        self.n_items += 1
        return item