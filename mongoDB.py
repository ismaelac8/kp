import pymongo
from pymongo.collection import Collection
import logging
import time
from bson import ObjectId
from datetime import datetime
import os
import json

def _convert_bson_fields(file_data):
    for item in file_data:
        fields = item.keys()
        for field in fields:
            if isinstance(item[field], dict):
                if '$oid' in item[field]:
                    item[field] = ObjectId(item[field]['$oid'])
                elif '$date' in item[field]:
                    item[field] = datetime.strptime(item[field]['$date'],
                                                    '%Y-%m-%dT%H:%M:%S.%fZ')

class MongoDatabase:

    def __init__(self,
                mongo_client_URI=None,
                mongo_database=None,
                collection_name=None,
                field_name=None,
                data=None,
                mongo_data_dict=None):
        self.mongo_client_URI = mongo_client_URI
        self.mongo_database = mongo_database
        self.collection_name = collection_name
        self.field_name = field_name
        self.data = data
        self.mongo_data_dict = mongo_data_dict
        
    def connect_to_mongo_db(self):
        try:
            self.myclient = pymongo.MongoClient(self.mongo_client_URI)
            logging.info("Connected to Mongo DB :{}".format(self.myclient))
        except Exception as E :
            raise Exception(f"An Exception occurred while connecting to Mongo DB : [{E}]")

    def close_mongo_db_connection(self):
        try:
            self.myclient.close()      
            logging.info("Closing Mongo DB connection")        
        except Exception as E :
            raise Exception(f"An Exception occurred while closing Mongo DB connection : [{E}]")
        
    def insert_mongo_data(self, convert_to_bson=False):
        try:
            self.connect_to_mongo_db()
            database = self.myclient[self.mongo_database]
            for key,value in self.mongo_data_dict.items():
                collection = database[key]
                data_file = os.path.dirname(os.path.abspath(__file__))+'/../../../mongo_data/'+value
                with open(data_file) as file:
                    file_data = json.load(file)
                    if convert_to_bson:
                        _convert_bson_fields(file_data)
                    try:
                        resp = collection.insert_many(file_data,ordered=False,bypass_document_validation=True)
                        logging.info(f"Bulk insert id {resp.inserted_ids}")   
                    except pymongo.errors.BulkWriteError as e:
                        print(e)
                        logging.info(f"Exception {e.details['writeErrors']}")
                        error_list = list(filter(lambda x: x['code'] != 11000, e.details['writeErrors']))
                        if len(error_list) > 0:
                            logging.info(f"Error occurred while inserting data : {error_list}")
            logging.info("Test data insertion completed successfully")
            return True
        except Exception as E:
            raise Exception(f"An Exception occurred while inserting test data to MongoDB : [{E}]")
       
    def fetch_data_with_field(self, collection_name, query):
        try:
            time.sleep(2)
            self.connect_to_mongo_db()
            database = self.myclient[self.mongo_database]
            collection = database[collection_name]
            logging.info(f" query {query}")
            q_response = collection.find(query)
            logging.info(f"qresponse {q_response}")
            for value in q_response: 
                logging.info("Response from MongoDB:{}".format(value))
                self.close_mongo_db_connection()
                return value
            return None
        except Exception as E:
            raise Exception(f"An Exception occurred while fetching data from MongoDB : [{E}]")

    def get_collection_by_name(self, collection_name: str):
        return self.myclient[self.mongo_database][collection_name]

    def print_collection(self, collection: Collection):
        documents = collection.find()
        for document in documents:
            print(document)

    def print_collection_by_name(self, collection_name: str):
        print(f"PRINTING COLLECTION: {collection_name}")
        collection = self.get_collection_by_name(collection_name)
        self.print_collection(collection)

    def delete_from_collection(self, collection: Collection, entry_id: int):
        id_filter = {"_id": entry_id}
        collection.delete_one(id_filter)

    def get_document_by_id(self, collection: Collection, document_id: int):
        id_filter = {"_id": document_id}
        document = collection.find_one(id_filter)
        return document
