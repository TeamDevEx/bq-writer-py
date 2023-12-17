import os
import json
import uuid
import config

from datetime import datetime

from flask import Flask, jsonify, request , g
from google.cloud import bigquery
from google.cloud import storage
from dotenv import load_dotenv

app = Flask(__name__)
load_dotenv()

bucket_name = os.getenv("BUCKET")

bigquery_client = bigquery.Client()
storage_client = storage.Client()

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
    event = request.get_json()
    print(event['name'])
    bucket = storage_client.bucket(bucket_name)
    data = json.loads(bucket.blob(event['name']).download_as_text() )     
    ingestion_time = datetime.utcnow().isoformat() 
    payload = {
        "ingestion_time": ingestion_time,
        "data": json.dumps(data)
    }
    try:
        table_ref = bigquery_client.dataset(os.getenv("DATASET")).table(os.getenv("TABLE"))
        table = bigquery_client.get_table(table_ref)        
        check_errors = bigquery_client.insert_rows(table, [payload])
        if not check_errors:
            print("PAYLOAD WAS INGESTED SUCCESSFULLY", e) 
            return jsonify({"status": 200 , "message": "Payload ingested successfully"})
        else:
            return jsonify({"status": 500 , "message": "SOMETHING IS WRONG"})
    except Exception as e:
        print("SOMETHING WENT WRONG:", e)        
        return jsonify({"status": 500 , "message": "Internal Server Error:" , "error": e})
    
if __name__ == "__main__":
    PORT = int(os.getenv("PORT")) if os.getenv("PORT") else 8080
    app.run(host="127.0.0.1", port=PORT, debug=True)
