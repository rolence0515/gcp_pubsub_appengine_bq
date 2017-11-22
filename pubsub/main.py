# Copyright 2015 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START app]
import base64
import json
import logging
import os

from flask import current_app, Flask, render_template, request
from google.cloud import pubsub_v1


from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from oauth2client.service_account import ServiceAccountCredentials


#my pull pubsub moduel
import pub_sub_pull

#my store to bigquery moduel
import store_to_bq



app = Flask(__name__)

# Configure the following environment variables via app.yaml
# This is used in the push request handler to veirfy that the request came from
# pubsub and originated from a trusted source.
app.config['PUBSUB_VERIFICATION_TOKEN'] = "abcdef"
app.config['PUBSUB_TOPIC'] =  "source"
app.config['PROJECT'] = "fluent-opus-185403"
app.config['PUBSUB_SUBSCRIPTION_PULL'] = "pull"



PUBSUB_SCOPES = ["https://www.googleapis.com/auth/pubsub"]
PRIVATE_KEY_FILE_PATH = 'key.json'


# Global list to storage messages received by this instance.
MESSAGES = []

#init stackdrive logging
import google.cloud.logging 
client = google.cloud.logging.Client("fluent-opus-185403")
client.setup_logging(logging.INFO)

@app.route('/trylog', methods=['GET', 'POST'])
def trylog():
    logging.info("view index page")
    return "ok" , 200

# [START index]
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'GET':
        return render_template('index.html', messages=MESSAGES)

    # send out string text , must encode as utf8 first
    data = request.form.get('payload', 'Example payload').encode('utf-8')

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(
        current_app.config['PROJECT'],
        current_app.config['PUBSUB_TOPIC'])

    # here send out encoding text message
    publisher.publish(topic_path, data=data)

    # data is you type in html text, pure string here, not a json object
    logging.info("send message ")

    return 'OK', 200
# [END index]


# [START push]
# this is call by gcp pub sub platform
@app.route('/pubsub/push', methods=['POST'])
def pubsub_push():
    if (request.args.get('token', '') !=
            current_app.config['PUBSUB_VERIFICATION_TOKEN']):
        return 'Invalid request', 400

    # get data from pubsub, become json object , scheam look sample_message.json
    # perhaps fro secureity reason, data must decode by base64
    envelope = json.loads(request.data.decode('utf-8'))
    payload = base64.b64decode(envelope['message']['data'])

    logging.info("receive push message ")

    MESSAGES.append(payload)

    # Returning any 2xx status indicates successful receipt of the message.
    return 'OK', 200
# [END push]

# [START pull]
# this is call by app engine cron setting, see cron.yaml
@app.route('/pubsub/pull', methods=['GET', 'POST'])
def pubsub_pull():
    if (request.args.get('token', '') !=
            current_app.config['PUBSUB_VERIFICATION_TOKEN']):
        return 'Invalid request', 400

    logging.info("ready to pull message")

    #testmsgs = [{"time" : "2017-11-13T10:01:01", "act":"click", "value":1}]
    #store_to_bq.load_msg_to_bigquery(testmsgs)

    #pull batch msg
    msgs = pub_sub_pull.pull_pubsub_msg(PRIVATE_KEY_FILE_PATH, current_app.config['PROJECT'], current_app.config['PUBSUB_SUBSCRIPTION_PULL'])
    if msgs:
        logging.info("pull down message, count:{}".format(len(msgs)))
        json_msgs = [json.loads(m) for m in msgs]

        #batch store to bigquery
        store_to_bq.load_msg_to_bigquery(json_msgs)
        
        #log 
        #strmsg = "|".join(json.dumps(m) for m in json_msgs)
        #logging.info(strmsg)

    return 'OK', 200
# [END pull]


# this is render plotly
import plotly.graph_objs as go
import bq_to_plotly
import plotly
@app.route('/plotly')
def render_plotly():
    df = bq_to_plotly.query_bq_to_dataframe()
    s1 = go.Bar(
            x=df["round_time"],
            y=df["click"],
            name='click'
    )
    s2 = go.Bar(
            x=df["round_time"],
            y=df["view"],
            name='view'
    )
    data = [s1, s2]
    layout = go.Layout(barmode='group')
    graphic = {"data":data, "layout":layout}
    graphJSON = json.dumps(graphic, cls=plotly.utils.PlotlyJSONEncoder)
    return render_template('plotly.html', graphJSON=graphJSON)

@app.errorhandler(500)
def server_error(e):
    logging.exception('An error occurred during a request.')
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500


if __name__ == '__main__':
    
    # This is used when running locally. Gunicorn is used to run the
    # application on Google App Engine. See entrypoint in app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
# [END app]
