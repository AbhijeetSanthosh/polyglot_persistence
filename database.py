import datetime
from pymongo import MongoClient
import pyodbc  # For SQL Server connection

# MongoDB Setup
mongo_client = MongoClient("mongodb://localhost:27017")
mongo_db = mongo_client["streamflix"]

# SQL Server Setup
sql_conn_str = "DRIVER={SQL Server};SERVER=localhost;DATABASE=Streamflix;Trusted_Connection=yes;"
sql_conn = pyodbc.connect(sql_conn_str)
cursor = sql_conn.cursor()

# Insert viewing event to MongoDB
def insert_viewing_event(user_id, content_id, region_id, device_type):
    event = {
        "UserID": user_id,
        "ContentID": content_id,
        "RegionID": region_id,
        "ViewDate": datetime.datetime.utcnow(),
        "DeviceType": device_type
    }
    mongo_db.ViewingLogs.insert_one(event)
    print("Viewing event inserted into MongoDB:", event)

# Update top content collection based on viewing logs
def update_top_content():
    pipeline = [
        {"$group": {
            "_id": {"RegionID": "$RegionID", "ContentID": "$ContentID"},
            "ViewCount": {"$sum": 1}}
        },
        {"$sort": {"ViewCount": -1}},
        {"$limit": 10}
    ]

    results = list(mongo_db.ViewingLogs.aggregate(pipeline))

    mongo_db.TopContent.delete_many({})

    top_content_docs = [{
        "RegionID": item["_id"]["RegionID"],
        "ContentID": item["_id"]["ContentID"],
        "ViewCount": item["ViewCount"],
        "LastUpdated": datetime.datetime.utcnow()
    } for item in results]

    mongo_db.TopContent.insert_many(top_content_docs)
    print("Top content collection updated.")

# Update SQL Server caching logs based on MongoDB top content
def update_caching_logs():
    cursor.execute("DELETE FROM CachingLogs")  # Clear old data
    top_content = mongo_db.TopContent.find()
    for content in top_content:
        cursor.execute("""
            INSERT INTO CachingLogs (RegionID, ContentID, CacheDate)
            VALUES (?, ?, ?)
        """, content["RegionID"], content["ContentID"], datetime.datetime.utcnow())
    sql_conn.commit()
    print("SQL caching logs updated.")

# MAIN FUNCTION
if __name__ == "__main__":
    # Example usage:
    insert_viewing_event(1001, 501, 1, "Smart TV")
    update_top_content()
    update_caching_logs()

    # Close database connections
    cursor.close()
    sql_conn.close()
