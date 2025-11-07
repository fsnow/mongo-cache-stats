"""
MongoDB Cache Statistics Monitor (Streamlit Version)

This script provides real-time visualization of MongoDB WiredTiger cache statistics
for collections and indexes using an interactive web interface. It displays cache
distribution as a pie chart with configurable denominator options.

Usage:
    uv run streamlit run mongo_cache_stats_streamlit.py -- "mongodb://localhost:27017"
"""

import certifi
import sys
from pymongo import MongoClient
import time
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import atexit

# Validate command-line arguments
if len(sys.argv) < 2:
    print("Please provide a MongoDB connection string as a command-line argument.")
    sys.exit(1)

connection_string = sys.argv[1]

# Configure Streamlit page settings
st.set_page_config(page_title="MongoDB Cache Usage", layout="wide")
st.title("MongoDB Cache Usage")

# Custom CSS to make tabs larger and more prominent
st.markdown("""
    <style>
    /* Make tabs larger and more visible */
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
        margin-bottom: 20px;
    }

    .stTabs [data-baseweb="tab"] {
        font-size: 20px;
        font-weight: 600;
        padding: 16px 32px;
        background-color: #f0f2f6;
        border-radius: 8px 8px 0 0;
    }

    .stTabs [aria-selected="true"] {
        background-color: #4169E1;
        color: white;
    }

    .stTabs [data-baseweb="tab"]:hover {
        background-color: #5B7FE8;
        color: white;
    }
    </style>
    """, unsafe_allow_html=True)


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

    Raises:
        Exception: If connection cannot be established with either method
    """
    # First try without SSL
    try:
        client = MongoClient(conn_string, serverSelectionTimeoutMS=5000)
        # Test the connection with a quick command
        client.admin.command('ping')
        return client
    except Exception as e:
        # If that fails, try with SSL
        try:
            client = MongoClient(conn_string, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=5000)
            # Test the connection
            client.admin.command('ping')
            return client
        except Exception as ssl_error:
            raise Exception(f"Could not connect to MongoDB. Non-SSL error: {e}. SSL error: {ssl_error}")


# Establish MongoDB connection with auto-detection
try:
    client = get_mongodb_connection(connection_string)
    db = client.admin
except Exception as e:
    st.error(f"Failed to connect to MongoDB: {e}")
    st.stop()

# Register cleanup function to run on exit
def cleanup():
    """Clean up MongoDB connection on exit"""
    try:
        client.close()
        print("\nMongoDB connection closed. Goodbye!")
    except:
        pass

atexit.register(cleanup)

# Hard-coded overhead factors (to be refined with real customer data)
COLLECTION_CACHE_OVERHEAD = 1.25  # Collections use ~25% more space in cache than data size
INDEX_CACHE_OVERHEAD = 0.80       # Indexes use ~20% less space in cache than storage size


def get_collection_stats():
    """
    Retrieve detailed cache statistics for all collections and indexes across all databases.

    This function iterates through all databases and collections, gathering
    WiredTiger cache metrics for both collection data and individual indexes.
    Views and system collections are excluded from the results.

    Returns:
        tuple: (collection_data, detailed_stats, total_cache_size, total_used_cache)
            - collection_data (list): Simple list for pie chart with 'name' and 'inCache' keys
            - detailed_stats (list): Detailed list for table with additional metrics
            - total_cache_size (int): Total WiredTiger cache size in bytes
            - total_used_cache (int): Total cache currently in use
    """
    dbInfos = db.command({"listDatabases": 1, "nameOnly": True})
    collection_data = []  # For pie chart
    detailed_stats = []   # For table

    # Get total cache size
    server_status = db.command("serverStatus")
    total_cache_size = server_status["wiredTiger"]["cache"]["maximum bytes configured"]

    for dbInfo in dbInfos["databases"]:
        dbName = dbInfo["name"]
        current_db = client[dbName]
        collections = current_db.list_collections()

        for collection in collections:
            # Skip views as they don't have cache statistics
            if collection["type"] == "view":
                continue

            collectionName = collection["name"]

            # Skip system collections
            if collectionName.startswith("system."):
                continue

            collStats = current_db.command("collstats", collectionName)

            # Double-check for views in case they weren't properly filtered
            if "errmsg" in collStats and collStats["errmsg"] == "Collection stats not supported on views":
                continue

            # Extract collection-level cache statistics
            ns = f"{dbName}.{collectionName}"
            doc_cache_bytes = int(collStats["wiredTiger"]["cache"]["bytes currently in the cache"])
            data_size = collStats["size"]  # Uncompressed size
            storage_size = collStats.get("storageSize", data_size)  # Compressed size on disk
            total_index_size = collStats["totalIndexSize"]
            doc_count = collStats.get("count", 0)
            avg_doc_size = collStats.get("avgObjSize", 0)

            # Adjust cache bytes by removing overhead to get effective cached data
            adjusted_doc_cache = doc_cache_bytes / COLLECTION_CACHE_OVERHEAD

            # Calculate estimated documents in cache, using adjusted cache
            est_docs_in_cache = int(adjusted_doc_cache / avg_doc_size) if avg_doc_size > 0 else 0
            est_docs_in_cache = min(est_docs_in_cache, doc_count)  # Cap at total document count

            # Calculate % Cached using adjusted cache (removes overhead effect)
            doc_cache_pct = (adjusted_doc_cache / data_size * 100) if data_size > 0 else 0
            doc_cache_pct = min(doc_cache_pct, 100.0)  # Cap at 100%

            # Sum up all index cache sizes for this collection
            total_index_cache = 0
            for indexName, indexStats in collStats.get("indexDetails", {}).items():
                index_cache_bytes = int(indexStats["cache"]["bytes currently in the cache"])
                total_index_cache += index_cache_bytes

            index_cache_pct = (total_index_cache / total_index_size * 100) if total_index_size > 0 else 0
            total_cache = doc_cache_bytes + total_index_cache

            # Add to pie chart data (collection + all indexes combined)
            collection_data.append({"name": ns, "inCache": total_cache})

            # Add detailed collection row
            detailed_stats.append({
                "Namespace": ns,
                "Type": "Collection",
                "Index Name": "",
                "Cache Used": doc_cache_bytes,
                "Data Size": data_size,
                "Storage Size": storage_size,
                "% Cached": doc_cache_pct,
                "Total Docs": doc_count,
                "Avg Doc Size": avg_doc_size,
                "Est. Docs in Cache": est_docs_in_cache
            })

            # Add index-level cache statistics
            for indexName, indexStats in collStats.get("indexDetails", {}).items():
                index_cache_bytes = int(indexStats["cache"]["bytes currently in the cache"])
                index_size = collStats["indexSizes"].get(indexName, 0)

                # Adjust cache bytes by removing efficiency factor to get effective cached index
                adjusted_index_cache = index_cache_bytes / INDEX_CACHE_OVERHEAD

                # Calculate % Cached using adjusted cache
                index_cache_pct = (adjusted_index_cache / index_size * 100) if index_size > 0 else 0
                index_cache_pct = min(index_cache_pct, 100.0)  # Cap at 100%

                detailed_stats.append({
                    "Namespace": ns,
                    "Type": "Index",
                    "Index Name": indexName,
                    "Cache Used": index_cache_bytes,
                    "Data Size": None,
                    "Storage Size": index_size,
                    "% Cached": index_cache_pct,
                    "Total Docs": None,
                    "Avg Doc Size": None,
                    "Est. Docs in Cache": None
                })

    # Calculate total used cache
    total_used_cache = sum(item["inCache"] for item in collection_data)

    return collection_data, detailed_stats, total_cache_size, total_used_cache


def create_pie_chart(data, total_cache_size, denominator_choice):
    """
    Create an interactive pie chart showing cache distribution.

    Generates a donut chart (pie chart with hole) visualizing how cache is
    distributed across collections and indexes. Optionally includes unused
    cache space based on user selection.

    Args:
        data (list): List of dictionaries with 'name' and 'inCache' keys
        total_cache_size (int): Total WiredTiger cache size in bytes
        denominator_choice (str): Which denominator to use for the pie chart

    Returns:
        plotly.graph_objects.Figure: Configured pie chart figure ready for display
    """
    labels = [item["name"] for item in data]
    values = [item["inCache"] for item in data]

    sum_used_cache = sum(values)

    # Add unused cache slice if user selected Total WiredTiger Cache denominator
    if denominator_choice == "Total WiredTiger Cache":
        unused_cache = total_cache_size - sum_used_cache
        if unused_cache > 0:
            labels.append("Unused Cache")
            values.append(unused_cache)

    # Create donut chart with labels and values
    fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=.3)])
    fig.update_layout(height=800)
    return fig


# Main monitoring loop
try:
    # Collect current statistics (now includes total_cache_size and total_used_cache)
    collection_data, detailed_stats, total_cache_size, total_used_cache = get_collection_stats()

    # Display summary statistics at the top
    sum_used_cache = total_used_cache
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Cache Size", f"{total_cache_size:,} bytes")
    with col2:
        st.metric("Total Used Cache", f"{sum_used_cache:,} bytes")
    with col3:
        st.metric("Cache Usage", f"{sum_used_cache/total_cache_size:.2%}")

    # Display overhead factors
    st.caption(f"⚙️ Overhead factors: Collections {COLLECTION_CACHE_OVERHEAD}x | Indexes {INDEX_CACHE_OVERHEAD}x")

    st.divider()

    # Create tabs for different views
    tab1, tab2 = st.tabs(["📋 Table", "🥧 Pie Chart"])

    with tab1:
        # Display detailed table
        col_left, col_right = st.columns([3, 1])
        with col_left:
            st.subheader("Detailed Cache Statistics")
        with col_right:
            st.caption("↓ Sorted by Cache Used (click any column to re-sort)")

        # Convert to DataFrame for better display
        df = pd.DataFrame(detailed_stats)

        # Calculate the two additional percentage columns
        df["% of Total Cache"] = df["Cache Used"].apply(lambda x: (x / total_cache_size * 100) if total_cache_size > 0 else 0)
        df["% of Used Cache"] = df["Cache Used"].apply(lambda x: (x / total_used_cache * 100) if total_used_cache > 0 else 0)

        # Reorder columns to put new percentages right after Cache Used
        column_order = [
            "Namespace", "Type", "Index Name", "Cache Used",
            "% of Total Cache", "% of Used Cache",
            "Data Size", "Storage Size", "% Cached",
            "Total Docs", "Avg Doc Size", "Est. Docs in Cache"
        ]
        df = df[column_order]

        # Sort by Cache Used descending by default
        df = df.sort_values(by="Cache Used", ascending=False)

        # Format percentage columns
        df["% Cached"] = df["% Cached"].apply(lambda x: f"{x:.2f}%" if isinstance(x, (int, float)) else x)
        df["% of Total Cache"] = df["% of Total Cache"].apply(lambda x: f"{x:.2f}%" if isinstance(x, (int, float)) else x)
        df["% of Used Cache"] = df["% of Used Cache"].apply(lambda x: f"{x:.2f}%" if isinstance(x, (int, float)) else x)

        # Display the dataframe with sortable columns
        st.dataframe(
            df,
            width='stretch',
            hide_index=True,
            column_config={
                "Namespace": st.column_config.TextColumn("Namespace", width="medium"),
                "Type": st.column_config.TextColumn("Type", width="small"),
                "Index Name": st.column_config.TextColumn("Index Name", width="medium"),
                "Cache Used": st.column_config.NumberColumn("Cache Used", format="%d", help="Uncompressed bytes in cache"),
                "% of Total Cache": st.column_config.TextColumn("% of Total Cache", width="small", help="Cache Used / Total WiredTiger Cache Size"),
                "% of Used Cache": st.column_config.TextColumn("% of Used Cache", width="small", help="Cache Used / Total Cache in Use"),
                "Data Size": st.column_config.NumberColumn("Data Size", format="%d", help="Uncompressed data size (collections only)"),
                "Storage Size": st.column_config.NumberColumn("Storage Size", format="%d", help="Compressed size on disk"),
                "% Cached": st.column_config.TextColumn("% Cached", width="small", help="Cache Used / Data Size (collections) or Cache Used / Storage Size (indexes). Adjusted for overhead: Collections 1.25x factor, Indexes 0.80x factor."),
                "Total Docs": st.column_config.NumberColumn("Total Docs", format="%d"),
                "Avg Doc Size": st.column_config.NumberColumn("Avg Doc Size", format="%d"),
                "Est. Docs in Cache": st.column_config.NumberColumn("Est. Docs in Cache", format="%d", help="Estimated based on Cache Used / Avg Doc Size. Adjusted for overhead (1.25x factor), capped at total doc count.")
            }
        )

    with tab2:
        # Display pie chart
        st.subheader("Cache Distribution")

        # User interface control for denominator selection
        denominator_choice = st.radio(
            "Choose the denominator for percentage calculation:",
            ("Sum of Used Cache", "Total WiredTiger Cache"),
            horizontal=True
        )

        fig = create_pie_chart(collection_data, total_cache_size, denominator_choice)
        st.plotly_chart(fig)

    # Auto-refresh every 60 seconds
    time.sleep(60)
    st.rerun()

except Exception as e:
    st.error(f"Error occurred: {e}")
    print(f"\nError: {e}")
