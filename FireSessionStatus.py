
import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

from firebase_admin import firestore
from FirebaseConnector.FirestoreConnector import FirestoreConnector

class FireSessionStatus(FirestoreConnector):
    def __init__(self, project, credentials, collection):
        FirestoreConnector.__init__(self, project, credentials)
        self.collection = collection
    
    def update_query_status(self, session_id, query_id, status):
        data = {
            "_id": session_id,
            "queries": {query_id: status}
        }
        self.update_document(self.collection, data)

    def on_query_init(self, session_id, query_id, query):
        status = {
            '_id': query_id,
            'query': query,
            'status': "starting",
            'start': firestore.SERVER_TIMESTAMP,
            'ready': None
        }
        self.update_query_status(session_id, query_id, status)

    def on_query_start(self, session_id, query_id):
        status = {
            'status': "crawling",
        }
        self.update_query_status(session_id, query_id, status)

    def on_query_finalize(self, session_id, query_id):
        status = {
            'status': "finalize",
        }
        self.update_query_status(session_id, query_id, status)

    def on_query_finished(self, session_id, query_id, n_results):
        status = {
            'status': "ready",
            'result': n_results,
            'ready': firestore.SERVER_TIMESTAMP
        }
        self.update_query_status(session_id, query_id, status)