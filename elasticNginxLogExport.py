import csv
from elasticsearch import Elasticsearch, helpers
import logging
from datetime import datetime
import pytz  # UTC to Istanbul timezone conversion

# Set up logging for debugging and error handling
logging.basicConfig(level=logging.DEBUG)

# Set up the connection to Elasticsearch
es = Elasticsearch("http://10.0.0.38:32603/")

# Query to fetch logs from the last 7 days
query = {
    "query": {
        "range": {
            "@timestamp": {
                "gte": "now-7d",
                "lt": "now"
            }
        }
    }
}

# Try to perform the scan search with pagination
try:
    response = helpers.scan(es, query=query, index="cluster_foo-*")  # Adjust index if needed
except Exception as e:
    logging.error(f"Error while fetching logs from Elasticsearch: {e}")
    exit(1)

# List to store log entries for sorting
log_entries = []
istanbul_tz = pytz.timezone("Europe/Istanbul")

# Iterate over each log entry and store data in a list
for entry in response:
    try:
        # Extract log fields
        source = entry["_source"]
        timestamp = source.get("@timestamp", "N/A")

        if timestamp != "N/A":
            # Parse the timestamp to datetime object and convert to Istanbul timezone
            utc_time = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            istanbul_time = utc_time.astimezone(istanbul_tz)
            formatted_time = istanbul_time.strftime("%Y-%m-%d %H:%M:%S")
        else:
            formatted_time = "N/A"

        # Extract additional fields
        referrer = source.get("nginx.referrer", "N/A")
        path = source.get("nginx.path", "N/A")
        remote = source.get("nginx.remote", "N/A")
        request_length = source.get("nginx.request_length", "N/A")
        upstream_status = source.get("nginx.upstream_status", "N/A")
        request_time = source.get("nginx.request_time", "N/A")
        upstream_response_length = source.get("nginx.upstream_response_length", "N/A")

        # Add entry to list for later sorting
        log_entries.append((formatted_time, referrer, path, remote, request_length,
                            upstream_status, request_time, upstream_response_length))

    except KeyError as e:
        logging.warning(f"Missing expected field {e} in log entry")
    except Exception as e:
        logging.error(f"Error processing log entry: {e}")

# Sort the log entries by the formatted timestamp
log_entries.sort(key=lambda x: datetime.strptime(x[0], "%Y-%m-%d %H:%M:%S") if x[0] != "N/A" else datetime.min)

# Open CSV file for writing
with open("kibana_logs_sorted.csv", "w", newline="") as f:
    writer = csv.writer(f)

    # Write CSV header with all fields
    writer.writerow(["timestamp", "nginx.referrer", "nginx.path", "nginx.remote",
                     "nginx.request_length", "nginx.upstream_status",
                     "nginx.request_time", "nginx.upstream_response_length"])

    # Write sorted log entries to CSV
    for entry in log_entries:
        writer.writerow(entry)

logging.info("CSV file with sorted logs has been created successfully.")
