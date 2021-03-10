import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from firebase_admin import firestore

import os
from datetime import datetime


class FirestoreConnector():

    @classmethod
    def TimeStamp(cls):
        return firestore.SERVER_TIMESTAMP

    def __init__(self, project, path_cred):
        assert isinstance(project, str)
        assert isinstance(path_cred, str)
        assert os.path.isfile(
            path_cred), f"Invalide credential file_path: [{path_cred}] does not exist!"
        self.cred = credentials.Certificate(path_cred)
        self.url = f"https://{project}.firebaseio.com"
        if not len(firebase_admin._apps):
            self.app = firebase_admin.initialize_app(
                self.cred, {'databaseURL': self.url})
        else:
            self.app = firebase_admin.initialize_app(
                self.cred, {'databaseURL': self.url}, project)
        self.db = firestore.client(self.app)


    def get_document(self, coll, _id):
        doc_ref = self.db.collection(coll).document(_id)
        return doc_ref.get().to_dict()

    def get_document_where(self, coll, uid):
        docs = self.db.collection(coll).where('uid', '==', uid).stream()
        return [doc.to_dict() for doc in docs]

    def delete_document(self, coll, _id):
        if self.check_exists(coll, _id):
            self.db.collection(coll).document(_id).delete()
    

    def get_collection(self, coll):
        doc_ref = self.db.collection(coll)
        return [x.to_dict() for x in doc_ref.get()]


    def check_exists(self, coll, _id):
        doc_ref = self.db.collection(coll).document(_id)
        doc = doc_ref.get()
        return doc.exists


    def set_document(self, coll, data, existing=False):
        assert '_id' in data.keys() or 'uid' in data.keys(), "required field _id missing"
        _id = data['_id'] if '_id' in data.keys() else data['uid']
        doc_ref = self.db.collection(coll).document(_id)
        doc = doc_ref.get()
        if not doc.exists or existing:
            data['created'] = firestore.SERVER_TIMESTAMP
            doc_ref.set(data, merge=False)

    def update_document(self, coll, data):
        assert '_id' in data.keys() or 'uid' in data.keys(), "required field _id missing"
        _id = data['_id'] if '_id' in data.keys() else data['uid']
        doc_ref = self.db.collection(coll).document(_id)
        if not self.check_exists(coll, _id):
            data['created'] = firestore.SERVER_TIMESTAMP
        data['changed'] = firestore.SERVER_TIMESTAMP

        doc_ref.set(data, merge=True)

    def update_collection(self, coll, data):
        assert isinstance(data, list), "Expected a list of documents"
        for item in data:
            self.update_document(coll, item)