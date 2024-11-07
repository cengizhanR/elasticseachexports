import csv
from elasticsearch import Elasticsearch, helpers
import logging
from datetime import datetime
import pytz  # UTC to Istanbul timezone conversion

# Set up logging for debugging and error handling
logging.basicConfig(level=logging.DEBUG)

# Set up the connection to Elasticsearch
es = Elasticsearch("http://10.0.0.38:32603/")
index_name = "rastge-2024_11_06"
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
    response = helpers.scan(es, query=query, index=index_name)  # Adjust index if needed
except Exception as e:
    logging.error(f"Error while fetching logs from Elasticsearch: {e}")
    exit(1)

# List to store log entries for sorting
log_entries = []
istanbul_tz = pytz.timezone("Europe/Istanbul")

# To keep track of the earliest and latest timestamps for the filename
earliest_timestamp = None
latest_timestamp = None

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

            # Track the earliest and latest timestamps
            if earliest_timestamp is None or istanbul_time < earliest_timestamp:
                earliest_timestamp = istanbul_time
            if latest_timestamp is None or istanbul_time > latest_timestamp:
                latest_timestamp = istanbul_time
        else:
            formatted_time = "N/A"

        # Extract the 'message' field (this should be the log content you're interested in)
        message = source.get("message", "N/A")

        # Add entry to list for later sorting
        log_entries.append((formatted_time, message))

    except KeyError as e:
        logging.warning(f"Missing expected field {e} in log entry")
    except Exception as e:
        logging.error(f"Error processing log entry: {e}")

# Sort the log entries by the formatted timestamp once
log_entries.sort(key=lambda x: datetime.strptime(x[0], "%Y-%m-%d %H:%M:%S") if x[0] != "N/A" else datetime.min)

# If we have the earliest and latest timestamps, use them to create the filename
if earliest_timestamp and latest_timestamp:
    date_range = f"{earliest_timestamp.strftime('%Y-%m-%d_%H-%M')}_to_{latest_timestamp.strftime('%Y-%m-%d_%H-%M')}"
else:
    date_range = "unknown_date_range"

# Create the filename dynamically based on the date range
filename = f"logs_{index_name}_{date_range}.csv"

# Open CSV file for writing
with open(filename, "w", newline="") as f:
    writer = csv.writer(f)

    # Write CSV header with timestamp and message fields
    writer.writerow(["timestamp", "log_message"])

    # Write sorted log entries to CSV efficiently in a single write operation
    writer.writerows(log_entries)

logging.info(f"CSV file with sorted logs has been created successfully: {filename}")
