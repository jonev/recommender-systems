from neo4j import GraphDatabase
import json
from types import SimpleNamespace
import os
import time
import pandas as pd
import logging
logging.basicConfig(filename='info.log', level=logging.INFO)

class DbWriter:

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

    def predict_for_user(self, user):
        with self.driver.session() as session:
            result = session.write_transaction(self._predict_for_user_on_popularity, user)
            return result

    def cold_start(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._cold_start_on_popularity)
            return result
    
    def cold_start_with_categories(self, categories):
        with self.driver.session() as session:
            result = session.write_transaction(self._cold_start_with_categories_on_popularity, categories)
            return result
    
    def exists(self, user):
        with self.driver.session() as session:
            result = session.write_transaction(self._exists, user)
            return result


    @staticmethod
    def _create_event(tx, event):
        if event.title is None and event.url == "http://adressa.no":
            # event.title = "Frontpage"
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
    def _exists(tx, user):
        result = tx.run("Match (u:User) where u.id = $userId "
                        "return u", userId=user)
        return result.single()

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


        

    
    @staticmethod
    def _predict_for_user_on_popularity(tx, user):
        result = tx.run(
                        " match (u1 {id: $userId})-[r1:read]->(a)<-[r2:read]-(u2)"
                        " match (u2)-[r3:read]->(recommendation) where not (u1)-[:read]->(recommendation) "
                        " return distinct recommendation.url as url, r3.activeTime as activeTime"
                        " order by activeTime desc"
                        " limit 10", userId=user)
        return [record["url"] for record in result]
    
    @staticmethod
    def _cold_start_on_popularity(tx):
        result = tx.run(
                        " match (u1)-[r1:read]->(recommendation)"
                        " return distinct recommendation.url as url, r1.activeTime as activeTime"
                        " order by r1.activeTime desc"
                        " limit 10")
        return [record["url"] for record in result]
    
    @staticmethod
    def _cold_start_with_categories_on_popularity(tx, categories):
        result = tx.run(
                        " match (u1)-[r1:read]->(recommendation)-[rc:has_category]->(c:Category)"
                        " where c.name in $categories"
                        " return distinct recommendation.url as url, r1.activeTime as activeTime"
                        " order by r1.activeTime desc"
                        " limit 10", categories=categories)
        return [record["url"] for record in result]
    
    def predict_on_popularity(self, users, categories):
        predictions = []
        nr_of_users = len(users)
        nr = 0
        for user in users:
            logging.info("--------------------------------------")
            start_time = time.time()
            if self.exists(user):
                p = self.predict_for_user(user)
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
    db = DbWriter("bolt://localhost:7687", "neo4j", "")
    # Import data into database:
    total_time = time.time()
    (train, test) = db.get_file_paths("active1000", 0.7)
    # logging.info(train, test)
    db.import_data("active1000", train) 
    logging.info(f"Import data took: {((time.time() - total_time)/60.0)} minutes") 
    # Import data took: 290.89670590559643 minutes with frontpages
    # Import data took: 219.35234892368317 minutes without  frontpages
    # df_test = load_data("active1000", test[:1])
    # df_test = df_test.dropna()
    # logging.info(df_test)
    # Insert new article

    # for col in df_test.columns:
    #     logging.info(col)
    # df_user = df_test[df_test["userId"] == 'cx:13563753207631091420187:v4m7n38yvolp']
    # logging.info(df_user)


    # Run predictions
    # "Now" is: Saturday, December 31, 2016 23:00:27
    # oldest_read = 123 #1488330061 # Wednesday, March 1, 2017 1:01:01
    
    # Prediction on existing users
    # logging.info("\n")
    # users = ["cx:13563753207631091420187:v4m7n38yvolp"] #, "cx:hrrqrd7eclmjbd57:23tj2qhytnkt9", "cx:13233863419858515505:13cyrrnk4fgs"]
    # predictions = db.predict(users, oldest_read, None)
    # for prediction in predictions:
    #     logging.info(f"Title: {prediction[0]}, url: {prediction[1]}, read-time: {str(prediction[2])}, time of read: {str(prediction[3])}")
    # logging.info("\n")

    # Prediction on new user - cold start
    # logging.info("\n")
    # categories = ["sport", "okonomi", "nyheter"] # Simulates that a new user is picking some categories when creating the user
    # users = ["unknown"]
    # db.predict(users, oldest_read, categories)
    # db.close()
    # logging.info("\n")



"""
Recommendation:

match (u1 {id: 'cx:13563753207631091420187:v4m7n38yvolp'})-[r1:read]->(a)<-[r2:read]-(u2)
where a.title <> 'Frontpage'
match (u2)-[r3:read]->(recommendation) where not (u1)-[:read]->(recommendation) and recommendation.title <> "Frontpage"
return distinct recommendation.title,r3.activeTime
order by r3.activeTime desc
limit 10
"""

"""
Dataset info:
Earliest read: Saturday, December 31, 2016 23:00:27 -> epoch: 1483225227
Latest read: Friday, March 31, 2017 21:59:59 -> epoch: 1490997599
"""