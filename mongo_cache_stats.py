import certifi
import sys
from pymongo import MongoClient
import time
from tabulate import tabulate

if len(sys.argv) < 2:
    print("Please provide a MongoDB connection string as a command-line argument.")
    sys.exit(1)

connection_string = sys.argv[1]

# Try to connect with both methods
def get_mongodb_connection(conn_string):
    # First try without SSL
    try:
        client = MongoClient(conn_string, serverSelectionTimeoutMS=5000)
        # Test the connection with a quick command
        client.admin.command('ping')
        print("Connected successfully without SSL")
        return client
    except Exception as e:
        print(f"Non-SSL connection failed: {e}")
        
        # If that fails, try with SSL
        try:
            client = MongoClient(conn_string, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=5000)
            # Test the connection
            client.admin.command('ping')
            print("Connected successfully with SSL")
            return client
        except Exception as e:
            print(f"SSL connection also failed: {e}")
            print("Could not connect to MongoDB. Please check your connection string and network.")
            sys.exit(1)

# Get a connection using the auto-detection function
client = get_mongodb_connection(connection_string)

db = client.admin

# Retrieve server status information
serverStatus = db.command("serverStatus")
print("Server Status:")
print(serverStatus)

dbInfos = db.command({"listDatabases": 1, "nameOnly": True})

dbNames = []
for dbInfo in dbInfos["databases"]:
    dbName = dbInfo["name"]
    dbNames.append(dbName)

collectionInfos = []

for dbName in dbNames:
    db = client[dbName]
    collections = db.list_collections()

    for collection in collections:
        if collection["type"] == "view":
            continue

        collectionName = collection["name"]
        indexesSpec = db[collectionName].index_information()
        
        indexesInfo = []
        
        for indexName, indexSpec in indexesSpec.items():
            indexesInfo.append({
                "name": indexName,
                "inCache": 0,
                "cacheRead": 0,
                "cacheWrite": 0,
                "pagesUsed": 0
            })
        
        collectionInfos.append({
            "db": dbName,
            "coll": collectionName,
            "inCache": 0,
            "cacheRead": 0,
            "cacheWrite": 0,
            "pagesUsed": 0,
            "indexesInfo": indexesInfo
        })

reportTime = 60
indexIndent = "_" + " "*7 

while True:
    # escape sequence to clear the console
    print('\033[2J')
    headers = ["Collection", "Size", "Cached", "%age", "Delta", "Read", "Written", "Used"]
    table_data = []
    
    for collInfo in collectionInfos:
        db = client[collInfo["db"]]
        collStats = db.command("collstats", collInfo["coll"])
        
        if collInfo["coll"].startswith("system."):
            continue
        
        if "errmsg" in collStats and collStats["errmsg"] == "Collection stats not supported on views":
            continue
        
        inCache = int(collStats["wiredTiger"]["cache"]["bytes currently in the cache"])
        cacheRead = int(collStats["wiredTiger"]["cache"]["bytes read into cache"])
        cacheWrite = int(collStats["wiredTiger"]["cache"]["bytes written from cache"])
        pagesUsed = int(collStats["wiredTiger"]["cache"]["pages requested from the cache"])
        collSize = collStats["size"] + collStats["totalIndexSize"]
        
        # Compute diffs
        sizeDiff = int((inCache - collInfo["inCache"]) / reportTime)
        readDiff = int((cacheRead - collInfo["cacheRead"]) / reportTime)
        writeDiff = int((cacheWrite - collInfo["cacheWrite"]) / reportTime)
        pageUseDiff = int((pagesUsed - collInfo["pagesUsed"]) / reportTime)
        
        ns = collInfo["db"] + "." + collInfo["coll"]
        
        if collSize > 0:
            pc = int((inCache / collSize) * 100)
            table_data.append([ns, collSize, inCache, pc, sizeDiff, readDiff, writeDiff, pageUseDiff])
        else:
            continue

        collInfo["inCache"] = inCache
        collInfo["cacheRead"] = cacheRead
        collInfo["cacheWrite"] = cacheWrite
        collInfo["pagesUsed"] = pagesUsed
        
        # Print index stats
        for indexInfo in collInfo["indexesInfo"]:
            indexName = indexInfo["name"]
            
            indexStats = collStats["indexDetails"][indexName]
            indexInCache = int(indexStats["cache"]["bytes currently in the cache"])
            indexCacheRead = int(indexStats["cache"]["bytes read into cache"])
            indexCacheWrite = int(indexStats["cache"]["bytes written from cache"])
            indexPagesUsed = int(indexStats["cache"]["pages requested from the cache"])
            indexSize = collStats["indexSizes"][indexName]
            
            # Compute diffs
            sizeDiff = int((indexInCache - indexInfo["inCache"]) / reportTime)
            readDiff = int((indexCacheRead - indexInfo["cacheRead"]) / reportTime)
            writeDiff = int((indexCacheWrite - indexInfo["cacheWrite"]) / reportTime)
            pageUseDiff = int((indexPagesUsed - indexInfo["pagesUsed"]) / reportTime)
            
            nameTab = ns + " (index: " + indexName + ")"
            
            if indexSize > 0:
                pc = int((indexInCache / indexSize) * 100)
                table_data.append([nameTab, indexSize, indexInCache, pc, sizeDiff, readDiff, writeDiff, pageUseDiff])
            
            indexInfo["inCache"] = indexInCache
            indexInfo["cacheRead"] = indexCacheRead
            indexInfo["cacheWrite"] = indexCacheWrite
            indexInfo["pagesUsed"] = indexPagesUsed
    
    table_data = sorted(table_data, key=lambda kv: kv[2])
    print(tabulate(table_data, headers, tablefmt="grid"))
    
    time.sleep(reportTime)