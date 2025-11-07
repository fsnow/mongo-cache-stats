"""
MongoDB Cache Statistics Monitor (CLI Version)

This script provides real-time monitoring of MongoDB WiredTiger cache statistics
for collections and indexes. It displays cache usage in a tabular format with
delta calculations showing the rate of change over time.

Usage:
    uv run mongo_cache_stats.py "mongodb://localhost:27017"
"""

import certifi
import sys
from pymongo import MongoClient
import time
from tabulate import tabulate

# Validate command-line arguments
if len(sys.argv) < 2:
    print("Please provide a MongoDB connection string as a command-line argument.")
    sys.exit(1)

connection_string = sys.argv[1]


def get_mongodb_connection(conn_string):
    """
    Establish a MongoDB connection with automatic SSL detection.

    Attempts to connect to MongoDB first without SSL, then with SSL if the
    initial connection fails. This allows the tool to work with various
    MongoDB configurations automatically.

    Args:
        conn_string (str): MongoDB connection string

    Returns:
        MongoClient: Connected MongoDB client instance

    Exits:
        Terminates the program if connection cannot be established
    """
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


# Establish MongoDB connection using auto-detection
client = get_mongodb_connection(connection_string)

db = client.admin

# Retrieve and display initial server status information
serverStatus = db.command("serverStatus")
print("Server Status:")
print(serverStatus)

# Get list of all databases
dbInfos = db.command({"listDatabases": 1, "nameOnly": True})

dbNames = []
for dbInfo in dbInfos["databases"]:
    dbName = dbInfo["name"]
    dbNames.append(dbName)

# Initialize data structure to track collection and index statistics
collectionInfos = []

# Iterate through all databases and collections to build initial tracking structure
for dbName in dbNames:
    db = client[dbName]
    collections = db.list_collections()

    for collection in collections:
        # Skip views as they don't have cache statistics
        if collection["type"] == "view":
            continue

        collectionName = collection["name"]
        indexesSpec = db[collectionName].index_information()

        # Initialize tracking for each index
        indexesInfo = []

        for indexName, indexSpec in indexesSpec.items():
            indexesInfo.append({
                "name": indexName,
                "inCache": 0,
                "cacheRead": 0,
                "cacheWrite": 0,
                "pagesUsed": 0
            })

        # Add collection to tracking list with initial zero values
        collectionInfos.append({
            "db": dbName,
            "coll": collectionName,
            "inCache": 0,
            "cacheRead": 0,
            "cacheWrite": 0,
            "pagesUsed": 0,
            "indexesInfo": indexesInfo
        })

# Configuration
reportTime = 60  # Time interval in seconds between reports
indexIndent = "_" + " "*7  # Indentation for index rows (unused but kept for compatibility)

# Main monitoring loop
while True:
    # Clear the console for fresh output
    print('\033[2J')

    # Setup table headers
    headers = ["Collection", "Size", "Cached", "%age", "Delta", "Read", "Written", "Used"]
    table_data = []

    # Iterate through all tracked collections
    for collInfo in collectionInfos:
        db = client[collInfo["db"]]
        collStats = db.command("collstats", collInfo["coll"])

        # Skip system collections
        if collInfo["coll"].startswith("system."):
            continue

        # Skip views (double-check in case they weren't filtered earlier)
        if "errmsg" in collStats and collStats["errmsg"] == "Collection stats not supported on views":
            continue

        # Extract current cache statistics from WiredTiger metrics
        inCache = int(collStats["wiredTiger"]["cache"]["bytes currently in the cache"])
        cacheRead = int(collStats["wiredTiger"]["cache"]["bytes read into cache"])
        cacheWrite = int(collStats["wiredTiger"]["cache"]["bytes written from cache"])
        pagesUsed = int(collStats["wiredTiger"]["cache"]["pages requested from the cache"])
        collSize = collStats["size"] + collStats["totalIndexSize"]

        # Calculate per-second deltas based on previous measurement
        sizeDiff = int((inCache - collInfo["inCache"]) / reportTime)
        readDiff = int((cacheRead - collInfo["cacheRead"]) / reportTime)
        writeDiff = int((cacheWrite - collInfo["cacheWrite"]) / reportTime)
        pageUseDiff = int((pagesUsed - collInfo["pagesUsed"]) / reportTime)

        # Build fully qualified namespace (db.collection)
        ns = collInfo["db"] + "." + collInfo["coll"]

        # Calculate cache percentage and add to table
        if collSize > 0:
            pc = int((inCache / collSize) * 100)
            table_data.append([ns, collSize, inCache, pc, sizeDiff, readDiff, writeDiff, pageUseDiff])
        else:
            continue

        # Update stored values for next iteration's delta calculation
        collInfo["inCache"] = inCache
        collInfo["cacheRead"] = cacheRead
        collInfo["cacheWrite"] = cacheWrite
        collInfo["pagesUsed"] = pagesUsed

        # Process index-level statistics for this collection
        for indexInfo in collInfo["indexesInfo"]:
            indexName = indexInfo["name"]

            # Extract index-specific cache statistics
            indexStats = collStats["indexDetails"][indexName]
            indexInCache = int(indexStats["cache"]["bytes currently in the cache"])
            indexCacheRead = int(indexStats["cache"]["bytes read into cache"])
            indexCacheWrite = int(indexStats["cache"]["bytes written from cache"])
            indexPagesUsed = int(indexStats["cache"]["pages requested from the cache"])
            indexSize = collStats["indexSizes"][indexName]

            # Calculate per-second deltas for index
            sizeDiff = int((indexInCache - indexInfo["inCache"]) / reportTime)
            readDiff = int((indexCacheRead - indexInfo["cacheRead"]) / reportTime)
            writeDiff = int((indexCacheWrite - indexInfo["cacheWrite"]) / reportTime)
            pageUseDiff = int((indexPagesUsed - indexInfo["pagesUsed"]) / reportTime)

            # Build descriptive name showing this is an index entry
            nameTab = ns + " (index: " + indexName + ")"

            # Calculate cache percentage and add to table
            if indexSize > 0:
                pc = int((indexInCache / indexSize) * 100)
                table_data.append([nameTab, indexSize, indexInCache, pc, sizeDiff, readDiff, writeDiff, pageUseDiff])

            # Update stored values for next iteration's delta calculation
            indexInfo["inCache"] = indexInCache
            indexInfo["cacheRead"] = indexCacheRead
            indexInfo["cacheWrite"] = indexCacheWrite
            indexInfo["pagesUsed"] = indexPagesUsed

    # Sort table by cached bytes (column index 2) for easier analysis
    table_data = sorted(table_data, key=lambda kv: kv[2])

    # Display formatted table
    print(tabulate(table_data, headers, tablefmt="grid"))

    # Wait for next reporting interval
    time.sleep(reportTime)
