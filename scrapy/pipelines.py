from scrapy.exporters import PythonItemExporter
from datetime import datetime
import os, binascii
from ..FirestoreConnector import FirestoreConnector

def str2hex(text):
    return binascii.hexlify(text.encode()).decode('utf-8')

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
        self.collection = collection
        self.document = document
        self.items = []


    def update_status(self, status):
        data = {
            "_id": self.document,
            "queries": {self.query_id: status}
        }
        self.db.update_document(self.collection, data)

    def open_spider(self, spider):
        self.query_id = str2hex(spider.query)
        self.update_status({})
        query_status = {
            'query': spider.query,
            'status': "crawling",
            'result': None,
            'start': str(datetime.now()),
            'ready': None
        }
        self.update_status(query_status)

    def close_spider(self, spider):
        query_status = {
            'query': spider.query,
            'status': "ready",
            'result': len(set(self.items)),
            'ready': str(datetime.now())
        }
        self.update_status(query_status)

    def process_item(self, item, spider):
        self.items.append(item['_id'])
        return item


class FirebaseItemPipeline(FirebaseConnector):
    @classmethod
    def from_crawler(cls, crawler):
        SETTINGS = "FIREBASE_ITEM_SETTINGS"
        coll, cred, proj = FirebaseConnector.get_settings(crawler, SETTINGS)

        return cls(credentials=cred, project=proj, collection=coll)

    def __init__(self, credentials, project, collection):
        FirebaseConnector.__init__(self, credentials, project, collection)

    def process_item(self, item, spider):
        self.db.update_document(self.collection, dict(item))
        return item
