from neo4j import GraphDatabase
import json
from types import SimpleNamespace
import os
import time
import pandas as pd
import logging
logging.basicConfig(filename='info.log', level=logging.INFO)

class GraphRecommendationSystem:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def insert_events(self, events):
        with self.driver.session() as session:
            for event in events:
                session.write_transaction(self._create_event, event)

    def insert_categories(self, categories):
        with self.driver.session() as session:
            for (documentId, categoriesString) in categories:
                session.write_transaction(self._create_categories, documentId, categoriesString)

    def _predict_for_user_on_popularity(self, user, friends):
        with self.driver.session() as session:
            result = session.write_transaction(self.__predict_for_user_on_popularity, user, friends)
            return result

    def cold_start(self):
        with self.driver.session() as session:
            result = session.write_transaction(self.__cold_start_on_popularity)
            return result
    
    def cold_start_with_categories(self, categories):
        with self.driver.session() as session:
            result = session.write_transaction(self.__cold_start_with_categories_on_popularity, categories)
            return result
    
    def user_exists(self, user):
        with self.driver.session() as session:
            result = session.write_transaction(self._user_exists, user)
            return result
    
    def find_best_friends(self, user):
        with self.driver.session() as session:
            result = session.write_transaction(self._find_best_friends, user)
            return result
    
    def find_newest_to_friend(self, user, friend):
        with self.driver.session() as session:
            result = session.write_transaction(self._find_newest_to_friend, user, friend)
            return result


    @staticmethod
    def _create_event(tx, event):
        if event.title is None and event.url == "http://adressa.no":
            return
        if event.title is None:
            event.title = "Unknown"
        if event.activeTime is None: 
            event.activeTime = -1
        if event.publishtime is None:
            event.publishtime = "1970-01-01T00:00:00.000Z"
        if event.documentId is None:
            event.documentId = "Unknown"

        result = tx.run("Merge (u:User {id: $userId}) "
                        "Merge (a:Article {title: $title, url: $url, publishtime: $publishTime, documentId: $documentId}) "
                        "Merge (u)-[r:read {activeTime: $activeTime, eventId: $eventId, time: $time}]->(a) "
                        "RETURN id(a) as articleId", userId=event.userId, activeTime=event.activeTime, eventId=event.eventId, time=event.time, title=event.title, url=event.url, publishTime=int(time.mktime(time.strptime(event.publishtime, '%Y-%m-%dT%H:%M:%S.%fZ'))), documentId=event.documentId)
        return result.single()[0]
    
    @staticmethod
    def _create_categories(tx, documentId, categoriesString):
        categories = categoriesString.split("|")
        query = "Match (a) where a.documentId= $documentId "
        for category in categories:
            tx.run(query + " Merge (c:Category {name: $name}) "
                        "Merge (a)-[r:has_category]->(c) "
                        "RETURN id(c) as id", documentId=documentId, name=category)
    
    @staticmethod
    def _user_exists(tx, user):
        result = tx.run("Match (u:User) where u.id = $userId "
                        "return u", userId=user)
        return result.single()


    @staticmethod
    def __predict_for_user_on_popularity(tx, user, friends):
        result = tx.run(
                        " match (f:User)-[r:read]->(recommendation:Article) where f.id IN $friendIds"
                        " match (u:User {id: $userId})"
                        " where not (u)-[:read]->(recommendation:Article)"
                        " return distinct recommendation.url as url, r.activeTime as activeTime"
                        " order by activeTime desc"
                        " limit 20", userId=user, friendIds=friends)
        return [record["url"] for record in result]
    
    @staticmethod
    def __cold_start_on_popularity(tx):
        result = tx.run(
                        " match (u1)-[r1:read]->(recommendation)"
                        " return distinct recommendation.url as url, r1.activeTime as activeTime"
                        " order by r1.activeTime desc"
                        " limit 10")
        return [record["url"] for record in result]
    
    @staticmethod
    def __cold_start_with_categories_on_popularity(tx, categories):
        result = tx.run(
                        " match (u1)-[r1:read]->(recommendation)-[rc:has_category]->(c:Category)"
                        " where c.name in $categories"
                        " return distinct recommendation.url as url, r1.activeTime as activeTime"
                        " order by r1.activeTime desc"
                        " limit 10", categories=categories)
        return [record["url"] for record in result]
    
    
    def predict_on_popularity(self, users, categories=None):
        predictions = []
        nr_of_users = len(users)
        nr = 0
        for user in users:
            logging.info("--------------------------------------")
            start_time = time.time()
            if self.user_exists(user):
                friends = self.find_best_friends(user)
                p = self._predict_for_user_on_popularity(user, friends)
                predictions.append([user, p])
            else:
                logging.info(f"User: {user}, does not exist, running cold start") # In a real case, the user will exist, but it will only have been reading the front-page, this simulate the same behavior
                if categories is None:
                    colds = self.cold_start()
                else:
                    logging.info(f"With categories: {categories}")
                    colds = self.cold_start_with_categories(categories)

                predictions.append([user, colds])
            nr = nr + 1
            took_m = ((time.time() - start_time)/60.0)
            logging.info(f"User took: {took_m} minutes, {nr}/{nr_of_users}, estimated time left: {((nr_of_users - nr)*took_m)} minutes")
        p = []
        for prediction in predictions:
            for pp in prediction[1]:
                p.append([prediction[0], pp])
        predictions_df = pd.DataFrame(p)
        predictions_df.columns = ["userId", "url"]
        return predictions_df
    
    @staticmethod
    def _find_best_friends(tx, user):
        result = tx.run(
                        " match (u:User {id: $userId})-[r:read]->(a:Article)<-[:read]-(f:User)"
                        " return f.id as friend, count(*) as c"
                        " order by c desc"
                        " limit 10", userId=user)
        return [record["friend"] for record in result]
    
    @staticmethod
    def _find_newest_to_friend(tx, user, friends):
        result = tx.run(
                        " match (f:User)-[r:read]->(recommendation:Article) where f.id IN $friendIds"
                        " match (u:User {id: $userId})"
                        " where not (u)-[:read]->(recommendation:Article)"
                        " return recommendation.url as url, recommendation.publishtime as publishtime"
                        " order by publishtime desc"
                        " limit 20", userId=user, friendIds=friends)
        return [record["url"] for record in result]
    
    def predict_on_bestfriends_newest(self, users):
        predictions = []
        nr_of_users = len(users)
        nr = 0
        for user in users:
            logging.info("--------------------------------------")
            start_time = time.time()
            friends = self.find_best_friends(user)
            predictions.append([user, self.find_newest_to_friend(user, friends)])
            took_m = ((time.time() - start_time)/60.0)
            logging.info(f"User took: {took_m} minutes, {nr}/{nr_of_users}, estimated time left: {((nr_of_users - nr)*took_m)} minutes")
            nr = nr + 1
        p = []
        for prediction in predictions:
            for pp in prediction[1]:
                p.append([prediction[0], pp])
        predictions_df = pd.DataFrame(p)
        predictions_df.columns = ["userId", "url"]
        return predictions_df

    def import_data(self, path, files):
        nrOfFiles = len(files)
        nr = 0
        logging.info(f"Starting import, nr of files: {nrOfFiles}")
        for f in files:
            file_name=os.path.join(path,f)
            nr = nr + 1
            if os.path.isfile(file_name):
                start_time = time.time()
                logging.info(f"Filename: {file_name}, nr: {nr}/{nrOfFiles}")
                events = []
                categories = []
                for line in open(file_name):
                    event = json.loads(line, object_hook=lambda d: SimpleNamespace(**d))
                    if not event is None:
                        events.append(event)
                        if event.category is not None:
                            categories.append([event.documentId, event.category])
                self.insert_events(events)
                self.insert_categories(categories)
                logging.info(f"File took: {((time.time() - start_time)/60.0)} minutes")
    
    def get_file_paths(self, root_directory: str, test_factor: float):
        all_files = os.listdir(root_directory)
        all_files.sort()
        nr_of_files = len(all_files)
        logging.info(all_files)
        logging.info(f"Number of files: {nr_of_files}")
        train = int(nr_of_files * test_factor)
        test = nr_of_files - train
        logging.info(f"Split dataset - files, train: {train}, test: {test}")
        return (all_files[:train], all_files[train:])

def load_data(path, files):
    map_lst=[]
    nrOfFiles = len(files)
    nr = 0
    logging.info(f"Starting import, nr of files: {nrOfFiles}")
    for f in files:
        file_name=os.path.join(path,f)
        nr = nr + 1
        logging.info(f"Filename: {file_name}, nr: {nr}/{nrOfFiles}")
        if os.path.isfile(file_name):
            for line in open(file_name):
                obj = json.loads(line.strip())
                if not obj is None:
                    map_lst.append(obj)
    return pd.DataFrame(map_lst) 

if __name__ == "__main__":
    logging.info("Please use the notebook 'graph_base_db'")
