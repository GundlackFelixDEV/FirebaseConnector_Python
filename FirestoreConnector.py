import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from firebase_admin import firestore

import os


class FirestoreConnector():
    def __init__(self, project, path_cred):
        assert isinstance(project, str)
        assert isinstance(path_cred, str)
        assert os.path.isfile(path_cred)
        self.cred = credentials.Certificate(path_cred)
        self.url = f"https://{project}.firebaseio.com"

    def connect(self):
        self.app = firebase_admin.initialize_app(
            self.cred, {'databaseURL': self.url})
        self.db = firestore.client()

    def get_document(self, name, _id):
        doc_ref = self.db.collection(name).document(_id)
        return doc_ref.get().to_dict()

    def get_collection(self, name):
        doc_ref = self.db.collection(name)
        return [x.to_dict() for x in doc_ref.get()]

    def update_collection(self, name, data):
        assert '_id' in data.keys()
        doc_ref = self.db.collection(name).document(data['_id'])
        doc_ref.set(data)
