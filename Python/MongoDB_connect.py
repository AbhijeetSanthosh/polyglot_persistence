from pymongo import MongoClient

# MongoDB Atlas connection string
connection_string = "mongodb+srv://businessdarvesh:asdfghjkl123@groupr.gpmjz.mongodb.net/?retryWrites=true&w=majority&appName=GroupR"

try:
    # Create a MongoClient instance using the connection string
    client = MongoClient(connection_string)
    client.admin.command('ping')
    print("Successfully connected to MongoDB Atlas!")

    db = client["streamflix"]
    viewing_logs = db["ViewingLogs"]
    top_content = db["TopContent"]
    
    # Use the 'ping' command to verify connection
    client.admin.command('ping')
    print("Successfully connected to MongoDB Atlas!")
    
except Exception as e:
    print("Unable to connect:", e)
