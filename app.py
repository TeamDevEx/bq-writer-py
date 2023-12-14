import os
import json
import uuid
import config

from datetime import datetime

from flask import Flask, jsonify, request , g
from google.cloud import bigquery


app = Flask(__name__)

bigquery_client = bigquery.Client()

@app.before_request
def before_request_func():
    execution_id = uuid.uuid4()
    g.start_time = datetime.utcnow().isoformat()  
    g.execution_id = execution_id
    print(g.execution_id, "ROUTE CALLED ", request.url)

@app.after_request
def after_request(response):
    if response and response.get_json():
        data = response.get_json()
        data["time_request"] = datetime.utcnow().isoformat()  
        data["version"] = config.VERSION
        response.set_data(json.dumps(data))

    return response
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
        return jsonify({"status": 200 , "message": "Payload ingested successfully"})
    except Exception as e:
        print("SOMETHING WENT WRONG:", e)        
        return jsonify({"status": 500 , "message": "Internal Server Error"})
    
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", config.PORT)))
    
