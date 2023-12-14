import os
import json
from datetime import datetime

from flask import Flask, request
from google.cloud import bigquery


app = Flask(__name__)

bigquery_client = bigquery.Client()

@app.route("/cloudEvent", methods=["POST"])
def cloud_event():
    timestamp = datetime.utcnow().isoformat()    
    event = request.get_json()

    payload = {
        "ingestion_time": timestamp,
        "data": json.dumps(event),
    }

    try:
        dataset_ref = bigquery_client.dataset("dora")
        table_ref = dataset_ref.table("batch_events")
        table = bigquery_client.get_table(table_ref)
        rows = [payload]
        bigquery_client.insert_rows(table, rows)

        print("PAYLOAD WAS INGESTED SUCCESSFULLY")
        return "Payload ingested successfully", 200
    except Exception as e:
        print("SOMETHING WENT WRONG:", e)        
        return "Internal Server Error", 500
    
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
