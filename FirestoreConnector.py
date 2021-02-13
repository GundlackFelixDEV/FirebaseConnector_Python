import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from firebase_admin import firestore

import os
from datetime import datetime


class FirestoreConnector():
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
        
    def delete_document(self, coll, _id):
        self.db.collection(coll).doc(_id).delete()
    
    def get_collection(self, coll):
        doc_ref = self.db.collection(coll)
        return [x.to_dict() for x in doc_ref.get()]

    def check_exists(self, coll, data):
        assert '_id' in data.keys()
        doc_ref = self.db.collection(coll).document(data['_id'])
        doc = doc_ref.get()
        return doc.exists

    def update_collection(self, coll, data):
        assert '_id' in data.keys()
        doc_ref = self.db.collection(coll).document(data['_id'])
        if not self.check_exists(coll, data):
            data['created'] = str(datetime.now())
        data['changed'] = str(datetime.now())

        doc_ref.set(data, merge=True)
