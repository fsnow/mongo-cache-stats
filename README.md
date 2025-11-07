# MongoDB Cache Stats

A real-time monitoring tool for MongoDB WiredTiger cache statistics. Track collection and index cache usage with both command-line and web interfaces.

## Features

- **Real-time Monitoring**: Live updates of cache statistics every 60 seconds
- **Collection & Index Tracking**: Monitor cache usage for both collections and their indexes
- **Two Interfaces**:
  - **CLI**: Tabular display with delta calculations showing cache changes over time
  - **Streamlit Web App**: Interactive pie chart visualization of cache distribution
- **Auto-SSL Detection**: Automatically detects whether SSL/TLS is required for connection
- **WiredTiger Metrics**: Tracks bytes in cache, read/write operations, and page requests

## Installation

This project uses [uv](https://github.com/astral-sh/uv) for package management.

```bash
# Clone the repository
git clone <repository-url>
cd mongo-cache-stats

# Install dependencies (uv will handle this automatically when running)
uv sync
```

## Usage

### Command-Line Interface

The CLI version displays cache statistics in a tabular format with real-time deltas:

```bash
uv run mongo_cache_stats.py "mongodb://localhost:27017"
```

**Output columns:**
- **Collection**: Database and collection name (includes indexes)
- **Size**: Total size of collection + indexes
- **Cached**: Bytes currently in cache
- **%age**: Percentage of total size that's cached
- **Delta**: Change in cached bytes per second
- **Read**: Bytes read into cache per second
- **Written**: Bytes written from cache per second
- **Used**: Page requests from cache per second

### Streamlit Web Interface

The Streamlit version provides an interactive pie chart visualization:

```bash
uv run streamlit run mongo_cache_stats_streamlit.py -- "mongodb://localhost:27017"
```

**Features:**
- Interactive pie chart showing cache distribution across collections and indexes
- Toggle between "Sum of Used Cache" and "Total WiredTiger Cache" denominators
- Real-time updates with total cache size and usage percentage
- Auto-refresh every 60 seconds

## Connection String Examples

```bash
# Local MongoDB (no authentication)
uv run mongo_cache_stats.py "mongodb://localhost:27017"

# MongoDB with authentication
uv run mongo_cache_stats.py "mongodb://username:password@localhost:27017"

# MongoDB Atlas
uv run mongo_cache_stats.py "mongodb+srv://username:password@cluster.mongodb.net/"

# Replica Set
uv run mongo_cache_stats.py "mongodb://host1:27017,host2:27017,host3:27017/?replicaSet=myReplSet"
```

## Requirements

- Python >= 3.13
- MongoDB with WiredTiger storage engine
- Dependencies (managed by uv):
  - pymongo >= 4.11.3
  - certifi >= 2025.1.31
  - tabulate >= 0.9.0
  - streamlit >= 1.41.1 (for web interface)
  - plotly >= 5.24.1 (for web interface)

## How It Works

The tool connects to your MongoDB instance and periodically collects cache statistics from:
- `serverStatus` command for overall server metrics
- `collStats` command for collection-level cache information
- `indexDetails` within collStats for index-specific cache data

The CLI version calculates deltas between measurements to show the rate of change, helping you identify actively used collections and indexes. The Streamlit version provides a visual breakdown of how your cache is being utilized across your database.

## SSL/TLS Support

The tool automatically detects SSL/TLS requirements:
1. First attempts connection without SSL
2. If that fails, retries with SSL using system certificates via certifi
3. Reports which method was successful

## Use Cases

- **Performance Monitoring**: Identify which collections and indexes are consuming cache
- **Cache Optimization**: Determine if your cache size is adequate for your workload
- **Index Analysis**: See which indexes are most frequently accessed
- **Capacity Planning**: Track cache usage trends over time to inform hardware decisions

## License

[Add your license here]

## Contributing

[Add contribution guidelines here]
