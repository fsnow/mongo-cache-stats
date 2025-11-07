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

# Hard-coded overhead factors (to be refined with real customer data)
COLLECTION_CACHE_OVERHEAD = 1.25  # Collections use ~25% more space in cache than data size
INDEX_CACHE_OVERHEAD = 0.80       # Indexes use ~20% less space in cache than storage size

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

        # Get initial collection stats for data/storage size
        try:
            collStats = db[collectionName].command("collstats", collectionName)
            data_size = collStats.get("size", 0)
            storage_size = collStats.get("storageSize", data_size)
            doc_count = collStats.get("count", 0)
            avg_doc_size = collStats.get("avgObjSize", 0)
        except:
            data_size = 0
            storage_size = 0
            doc_count = 0
            avg_doc_size = 0

        # Add collection to tracking list with initial zero values
        collectionInfos.append({
            "db": dbName,
            "coll": collectionName,
            "inCache": 0,
            "cacheRead": 0,
            "cacheWrite": 0,
            "pagesUsed": 0,
            "dataSize": data_size,
            "storageSize": storage_size,
            "docCount": doc_count,
            "avgDocSize": avg_doc_size,
            "indexesInfo": indexesInfo
        })

# Configuration
reportTime = 60  # Time interval in seconds between reports
indexIndent = "_" + " "*7  # Indentation for index rows (unused but kept for compatibility)

# Main monitoring loop
while True:
    # Clear the console for fresh output
    print('\033[2J')

    # Get total cache size for percentage calculations
    server_status = db.command("serverStatus")
    total_cache_size = server_status["wiredTiger"]["cache"]["maximum bytes configured"]

    # Setup table headers
    headers = ["Namespace", "Type", "Cache Used", "Data Size", "Storage Size", "% Cached", "Delta", "Read", "Written", "Used"]
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
        doc_cache_bytes = int(collStats["wiredTiger"]["cache"]["bytes currently in the cache"])
        cacheRead = int(collStats["wiredTiger"]["cache"]["bytes read into cache"])
        cacheWrite = int(collStats["wiredTiger"]["cache"]["bytes written from cache"])
        pagesUsed = int(collStats["wiredTiger"]["cache"]["pages requested from the cache"])

        # Get size information
        data_size = collStats.get("size", 0)
        storage_size = collStats.get("storageSize", data_size)

        # Update stored values for size tracking
        collInfo["dataSize"] = data_size
        collInfo["storageSize"] = storage_size

        # Calculate per-second deltas based on previous measurement
        sizeDiff = int((doc_cache_bytes - collInfo["inCache"]) / reportTime)
        readDiff = int((cacheRead - collInfo["cacheRead"]) / reportTime)
        writeDiff = int((cacheWrite - collInfo["cacheWrite"]) / reportTime)
        pageUseDiff = int((pagesUsed - collInfo["pagesUsed"]) / reportTime)

        # Build fully qualified namespace (db.collection)
        ns = collInfo["db"] + "." + collInfo["coll"]

        # Adjust cache bytes by removing overhead to get effective cached data
        adjusted_doc_cache = doc_cache_bytes / COLLECTION_CACHE_OVERHEAD

        # Calculate % Cached using adjusted cache (removes overhead effect)
        doc_cache_pct = (adjusted_doc_cache / data_size * 100) if data_size > 0 else 0
        doc_cache_pct = min(doc_cache_pct, 100.0)  # Cap at 100%

        # Add collection row to table
        if data_size > 0:
            table_data.append([ns, "Collection", doc_cache_bytes, data_size, storage_size,
                             f"{doc_cache_pct:.1f}%", sizeDiff, readDiff, writeDiff, pageUseDiff])
        else:
            continue

        # Update stored values for next iteration's delta calculation
        collInfo["inCache"] = doc_cache_bytes
        collInfo["cacheRead"] = cacheRead
        collInfo["cacheWrite"] = cacheWrite
        collInfo["pagesUsed"] = pagesUsed

        # Process index-level statistics for this collection
        for indexInfo in collInfo["indexesInfo"]:
            indexName = indexInfo["name"]

            # Extract index-specific cache statistics
            indexStats = collStats["indexDetails"][indexName]
            index_cache_bytes = int(indexStats["cache"]["bytes currently in the cache"])
            indexCacheRead = int(indexStats["cache"]["bytes read into cache"])
            indexCacheWrite = int(indexStats["cache"]["bytes written from cache"])
            indexPagesUsed = int(indexStats["cache"]["pages requested from the cache"])
            index_size = collStats["indexSizes"][indexName]

            # Calculate per-second deltas for index
            sizeDiff = int((index_cache_bytes - indexInfo["inCache"]) / reportTime)
            readDiff = int((indexCacheRead - indexInfo["cacheRead"]) / reportTime)
            writeDiff = int((indexCacheWrite - indexInfo["cacheWrite"]) / reportTime)
            pageUseDiff = int((indexPagesUsed - indexInfo["pagesUsed"]) / reportTime)

            # Adjust cache bytes by removing efficiency factor to get effective cached index
            adjusted_index_cache = index_cache_bytes / INDEX_CACHE_OVERHEAD

            # Calculate % Cached using adjusted cache
            index_cache_pct = (adjusted_index_cache / index_size * 100) if index_size > 0 else 0
            index_cache_pct = min(index_cache_pct, 100.0)  # Cap at 100%

            # Build namespace with index indicator
            index_ns = f"{ns}.{indexName}"

            # Add index row to table
            if index_size > 0:
                table_data.append([index_ns, "Index", index_cache_bytes, None, index_size,
                                 f"{index_cache_pct:.1f}%", sizeDiff, readDiff, writeDiff, pageUseDiff])

            # Update stored values for next iteration's delta calculation
            indexInfo["inCache"] = index_cache_bytes
            indexInfo["cacheRead"] = indexCacheRead
            indexInfo["cacheWrite"] = indexCacheWrite
            indexInfo["pagesUsed"] = indexPagesUsed

    # Sort table by cached bytes (column index 2) descending for easier analysis
    table_data = sorted(table_data, key=lambda kv: kv[2], reverse=True)

    # Display formatted table
    print(tabulate(table_data, headers, tablefmt="grid"))

    # Wait for next reporting interval
    time.sleep(reportTime)
