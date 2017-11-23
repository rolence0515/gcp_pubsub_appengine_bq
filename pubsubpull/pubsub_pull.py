import base64
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery
import logging
import time

#const var
PUBSUB_SCOPES = ["https://www.googleapis.com/auth/pubsub"]
BOTNAME = 'pubsub-irc-bot/1.0'
NUM_RETRIES = 3
BATCH_SIZE = 1000000000
MAX_BATCH_MSG_COUNT = 6000 #10W

#call this function return data array
#auto ack message 
def pull_pubsub_msg(keyfile, projectid, pull_subscription_name):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(keyfile, scopes=PUBSUB_SCOPES)
    client = discovery.build('pubsub', 'v1', credentials=credentials, cache_discovery=False)

    subscription = get_full_subscription_name(projectid, pull_subscription_name)
    body = {
        'returnImmediately': True,
        'maxMessages': BATCH_SIZE
    }
    try:
        
         # version 2
        empty  = False
        msgCount = 0
        result = []
        start = time.time()
        while empty == False and msgCount < MAX_BATCH_MSG_COUNT:
            # one bath pull call  only return 1000 msg
            resp = client.projects().subscriptions().pull(subscription=subscription, body=body).execute(num_retries=NUM_RETRIES)
            receivedMessages = resp.get('receivedMessages')

            if receivedMessages:
                #each item is tuple , tuple[0] is data , tuple[1] is ack
                tuple_array = [translate_data(m) for m in  receivedMessages]
                ack_ids = [t[1] for t in tuple_array]
                datas  = [t[0] for t in tuple_array]
                msgCount += len(datas)

                #act first for delete msg               
                ack_body = {'ackIds':ack_ids}
                client.projects().subscriptions().acknowledge(subscription=subscription, body=ack_body).execute(num_retries=NUM_RETRIES)
                
                logging.warning("receive msg count:{}, total:{}".format(len(datas), msgCount))
                result.extend(datas)
            else:
                empty = True

        end = time.time()
        hours, rem = divmod(end-start, 3600)
        minutes, seconds = divmod(rem, 60)
        logging.warning("process elapsed:{:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))
        return result

    except Exception as e:
        print("pull_pubsub_msg error", e)
        return []

#helper function 
def translate_data(msg):
    message = msg.get('message')
    if message:
        d = base64.b64decode(str(message.get('data'))).decode("utf-8")
        return (d, msg.get('ackId'))
    else:
        return (None, None)


def fqrn(resource_type, project, resource):
    """Return a fully qualified resource name for Cloud Pub/Sub."""
    return "projects/{}/{}/{}".format(project, resource_type, resource)


def get_full_subscription_name(project, subscription):
    """Return a fully qualified subscription name."""
    return fqrn('subscriptions', project, subscription)