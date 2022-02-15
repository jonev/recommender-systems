from neo4j import GraphDatabase
import json
from types import SimpleNamespace
import os
import time

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

    def predict_for_user(self, user, oldest_read):
        with self.driver.session() as session:
            result = session.write_transaction(self._predict_for_user, user, oldest_read)
            return result

    def cold_start(self, oldest_read):
        with self.driver.session() as session:
            result = session.write_transaction(self._cold_start, oldest_read)
            return result
    
    def cold_start_with_categories(self, oldest_read, categories):
        with self.driver.session() as session:
            result = session.write_transaction(self._cold_start_with_categories, oldest_read, categories)
            return result
    
    def exists(self, user):
        with self.driver.session() as session:
            result = session.write_transaction(self._exists, user)
            return result


    @staticmethod
    def _create_event(tx, event):
        if event.title is None and event.url == "http://adressa.no":
            event.title = "Frontpage"
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

    def import_data(self):
        path = "active1000"
        files = os.listdir(path)
        nrOfFiles = len(files)
        nr = 0
        print(f"Starting import, nr of files: {nrOfFiles}")
        for f in files:
            file_name=os.path.join(path,f)
            nr = nr + 1
            if os.path.isfile(file_name):
                start_time = time.time()
                print(f"Filename: {file_name}, nr: {nr}/{nrOfFiles}")
                events = []
                categories = []
                for line in open(file_name):
                    event = json.loads(line, object_hook=lambda d: SimpleNamespace(**d))
                    if not event is None:
                        events.append(event)
                        #articleId = self.insert_event(event)
                        if event.category is not None:
                            categories.append([event.documentId, event.category])
                            #self.insert_categories(articleId, event.category)
                self.insert_events(events)
                self.insert_categories(categories)
                print(f"File took: {((time.time() - start_time)/60.0)} minutes")

    
    @staticmethod
    def _predict_for_user(tx, user, oldest_read):
        result = tx.run(
                        " match (u1 {id: $userId})-[r1:read]->(a)<-[r2:read]-(u2)"
                        " where a.title <> 'Frontpage'" # At this point it would make sense to filter on r1.time to not recommend old articles
                        " match (u2)-[r3:read]->(recommendation) where not (u1)-[:read]->(recommendation) "
                        " and recommendation.title <> 'Frontpage' "
                        " and r3.time > $oldestRead"
                        " return distinct recommendation.title as title, recommendation.url as url,r3.activeTime as activeTime, r3.time as time"
                        " order by activeTime desc"
                        " limit 3", userId=user,oldestRead=oldest_read)
        return [[record["title"], record["url"], record["activeTime"], record["time"]] for record in result]
    
    @staticmethod
    def _cold_start(tx, oldest_read):
        result = tx.run(
                        " match (u1)-[r1:read]->(recommendation)"
                        " where recommendation.title <> 'Frontpage' and r1.time > $oldestRead"
                        " return distinct recommendation.title as title, recommendation.url as url, r1.activeTime as activeTime, r1.time as time"
                        " order by r1.activeTime desc"
                        " limit 3", oldestRead=oldest_read)
        return [[record["title"], record["url"], record["activeTime"], record["time"]] for record in result]
    
    @staticmethod
    def _cold_start_with_categories(tx, oldest_read, categories):
        result = tx.run(
                        " match (u1)-[r1:read]->(recommendation)-[rc:has_category]->(c:Category)"
                        " where recommendation.title <> 'Frontpage' and r1.time > $oldestRead and c.name in $categories"
                        " return distinct recommendation.title as title, recommendation.url as url, r1.activeTime as activeTime, r1.time as time, c.name as category"
                        " order by r1.activeTime desc"
                        " limit 3", oldestRead=oldest_read, categories=categories)
        return [[record["title"], record["url"], record["activeTime"], record["time"], record["category"]] for record in result]
    
    def predict(self, users, oldest_read, categories):
        for user in users:
            print("--------------------------------------")
            if self.exists(user):
                print(f"User Exists: {user}, running prediction")
                predictions = self.predict_for_user(user, oldest_read)
                for prediction in predictions:
                    print(f"Title: {prediction[0]}, url: {prediction[1]}, read-time: {str(prediction[2])}, time of read: {str(prediction[3])}")
            else:
                print(f"User: {user}, does not exist, running cold start") # In a real case, the user will exist, but it will only have been reading the front-page, this simulate the same behavior
                if categories is None:
                    colds = self.cold_start(oldest_read)
                else:
                    print(f"With categories: {categories}")
                    colds = self.cold_start_with_categories(oldest_read, categories)

                for cold in colds:
                    print(f"Title: {cold[0]}, url: {cold[1]}, read-time: {str(cold[2])}, time of read: {str(cold[3])}")



if __name__ == "__main__":
    db = DbWriter("bolt://graph:7687", "neo4j", "")
    # Import data into database:
    # total_time = time.time()
    # db.import_data()
    # print(f"File took: {((time.time() - total_time)/60.0)} minutes")

    # Insert new article



    # Run predictions
    # "Now" is: Saturday, December 31, 2016 23:00:27
    oldest_read = 123 #1488330061 # Wednesday, March 1, 2017 1:01:01
    
    # Prediction on existing users
    print("\n")
    users = ["cx:13563753207631091420187:v4m7n38yvolp", "cx:hrrqrd7eclmjbd57:23tj2qhytnkt9", "cx:13233863419858515505:13cyrrnk4fgs"]
    db.predict(users, oldest_read, None)
    print("\n")

    # Prediction on new user - cold start
    print("\n")
    categories = ["sport", "okonomi", "nyheter"] # Simulates that a new user is picking some categories when creating the user
    users = ["unknown"]
    db.predict(users, oldest_read, categories)
    db.close()
    print("\n")



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