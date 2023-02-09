"""
Da der Service, bei dem die API deployed wird, nur HTTP-Anfragen zulässt,
erfolgt der Zugriff auf MongoDB über die Atlas Data API.

Die dazugehörige Logik wurde in diese Pythondatei ausgelagert.
"""

import json
import requests

URL_FIND = "https://data.mongodb-api.com/app/data-dllci/endpoint/data/v1/action/find"
URL_INSERT_ONE = "https://data.mongodb-api.com/app/data-dllci/endpoint/data/v1/action/insertOne"

class MongoDBConnector:
    """
    Diese Klasse abstrahiert die Methoden zur Interkation mit 
    der MongoDB Atlas Data API: https://www.mongodb.com/docs/atlas/api/data-api/
    """

    def __init__(self, api_key):
        self.api_key = api_key

    def insert(self, collection, data):
        """
        Fügt ein Dokument in die Datenbank
        in die angegebene Collection ein.
        """
        payload = json.dumps({
            "collection": collection,
            "database": self.database,
            "dataSource": self.data_source,
            "document": data
        })

        headers = {
            "Content-Type": "application/json",
            "api-key": self.api_key,
            "Accept": "application/json" 
        }

        response = requests.request("POST", URL_INSERT_ONE, headers=headers, data=payload)
        results = json.loads(response.text)
        print(results)

    def find(self, collection, limit=20):
        """
        Sucht in der angegebenen Collection nach bis zu der 
        Anzahl der Elementen, die als Limit angegeben sind. 
        Dabei werden die Elemente der Eintragungszeit nach
        absteigend sortiert.
        """
        payload = json.dumps({
            "collection": collection,
            "database": self.database,
            "dataSource": self.data_source,
            "sort": { "_id": -1 },
            "limit": limit
        })

        headers = {
            "Content-Type": "application/json",
            "Access-Control-Request-Headers": "*",
            "api-key": self.api_key,
            "Accept": "application/json" 
        }

        response = requests.request("POST", URL_FIND, headers=headers, data=payload)
        results = json.loads(response.text)

        results = results["documents"]
        return results

    def set_database(self, database):
        self.database = database

    def set_data_source(self, data_source):
        self.data_source = data_source