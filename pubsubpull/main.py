import base64
import json
import os
import logging
from flask import Flask
from google.cloud import pubsub_v1
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from oauth2client.service_account import ServiceAccountCredentials

#import my python file
import pubsub_pull
import store_to_bq

#const setting
PUBSUB_VERIFICATION_TOKEN = "abcdef"
PROJECT = "fluent-opus-185403"
PUBSUB_SUBSCRIPTION_PULL = "pull"
PUBSUB_SCOPES = ["https://www.googleapis.com/auth/pubsub"]
PRIVATE_KEY_FILE_PATH = 'key.json'

#init stackdrive logging
import google.cloud.logging 
client = google.cloud.logging.Client(PROJECT)
client.setup_logging(logging.INFO)

app = Flask(__name__)

@app.route("/",methods=['GET'])
def index():
	return 'pubsub pull', 200

@app.route("/pubsub/pull",methods=['GET', 'POST'])
def pubsub_pull():

	logging.info("ready to pull message")
	msgs = pubsub_pull.pull_pubsub_msg(PRIVATE_KEY_FILE_PATH, PROJECT, PUBSUB_SUBSCRIPTION_PULL)
	
    if msgs:        
    	logging.info("total pull down message, count:{}".format(len(msgs)))
    	json_msgs = [json.loads(m) for m in msgs]

    	#batch store to bigquery
        #store_to_bq.analysis_msg_to_bq(json_msgs)
    return 'OK', 200


	
@app.errorhandler(500)
def server_error(e):
    logging.exception('An error occurred during a request.')
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500
	
if __name__ == "__main__":
	app.run(host="127.0.0.1", port=8080, debug=True)