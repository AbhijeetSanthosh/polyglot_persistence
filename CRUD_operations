from pymongo import MongoClient
import datetime

# MongoDB Atlas connection string (replace with your actual credentials)
connection_string = "mongodb+srv://businessdarvesh:asdfghjkl123@groupr.gpmjz.mongodb.net/?retryWrites=true&w=majority&appName=GroupR"

# Connect to MongoDB Atlas
try:
    client = MongoClient(connection_string)
    client.admin.command('ping')
    print("Successfully connected to MongoDB Atlas!")

    db = client["streamflix"]
    viewing_logs = db["ViewingLogs"]
    top_content = db["TopContent"]

except Exception as e:
    print("Unable to connect:", e)

# --- MongoDB CRUD operations ---

# Create (Insert Viewing Log)
def insert_viewing_log(user_id, content_id, region_id, device_type):
    log = {
        "UserID": user_id,
        "ContentID": content_id,
        "RegionID": region_id,
        "ViewDate": datetime.datetime.utcnow(),
        "DeviceType": device_type
    }
    viewing_logs = client["streamflix"]["ViewingLogs"]
    viewing_logs.insert_one(log)
    print("Viewing log inserted:", log)

# Read (Retrieve recent logs)
def get_recent_views(region_id, limit=5):
    viewing_logs = client["streamflix"]["ViewingLogs"]
    recent_views = viewing_logs.find({"RegionID": region_id}).sort("ViewDate", -1).limit(limit)
    return list(recent_views)

# Update Top Content based on Views
def update_top_content():
    viewing_logs = client["streamflix"]["ViewingLogs"]
    top_content = client["streamflix"]["TopContent"]
    
    pipeline = [
        {"$group": {
            "_id": {"RegionID": "$RegionID", "ContentID": "$ContentID"},
            "ViewCount": {"$sum": 1}}
        },
        {"$sort": {"ViewCount": -1}},
        {"$limit": 10}
    ]
    
    results = list(viewing_logs.aggregate(pipeline))
    top_content.delete_many({})

    top_content_docs = []
    for item in results:
        top_content_docs.append({
            "RegionID": item["_id"]["RegionID"],
            "ContentID": item["_id"]["ContentID"],
            "ViewCount": item["ViewCount"],
            "LastUpdated": datetime.datetime.utcnow()
        })

    top_content.insert_many(top_content_docs)
    print("Updated TopContent successfully.")

# Delete old viewing logs
def delete_old_logs(days=90):
    viewing_logs = client["streamflix"]["ViewingLogs"]
    cutoff_date = datetime.datetime.utcnow() - datetime.timedelta(days=days)
    deleted_count = viewing_logs.delete_many({"ViewDate": {"$lt": cutoff_date}}).deleted_count
    print(f"Deleted {deleted_count} old logs.")

# Example workflow usage
if __name__ == "__main__":
    import datetime
    
    # Insert viewing log
    insert_viewing_log(1001, 501, 1, "Smart TV")

    # Update top content
    update_top_content()

    # Get recent views for region 1
    recent_views = get_recent_views(region_id=1)
    print(recent_views)

    # Clean logs older than 90 days
    clean_old_logs(90)
