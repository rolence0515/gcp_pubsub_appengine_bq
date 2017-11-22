#coding=utf-8

import schedule
import time
import datetime
import random
from google.cloud import pubsub_v1
import json
import logging

#一秒打算多少訊息
ONE_SEC_BATCH_SIZE  = 100

#每隔多久送一次資料
SEND_PUBSUB_INTERVAL_SEC = 30

#打算每隔多少產生一批資料
GEN_BATCH_MSGS_INTERVAL_SEC = 10

import google.cloud.logging 
client = google.cloud.logging.Client("fluent-opus-185403")
client.setup_logging(logging.DEBUG)

on_gen_msg = False
msgs = []

def send_pubsub(msgs):
    publisher = pubsub_v1.PublisherClient()
    topic = publisher.topic_path("fluent-opus-185403", "source")
    for m in msgs:
        jsonMsg = json.dumps(m).encode('utf-8')
        publisher.publish(topic, data = jsonMsg)
    logging.info("send out msg to pubsub, count:{}".format(len(msgs)))


def send_job():
    while on_gen_msg:
        time.sleep(1)
    print("every",SEND_PUBSUB_INTERVAL_SEC, "sec, send to pubsub,  msg count:", len(msgs))
    print("head msg :" , msgs[0:5])
    send_pubsub(msgs)
    del msgs[:]
    
def gen_msg_job():
    on_gen_msg = True
    size = ONE_SEC_BATCH_SIZE * GEN_BATCH_MSGS_INTERVAL_SEC
    newmsgs = gen_msgs(size)
    print("every", GEN_BATCH_MSGS_INTERVAL_SEC, "sec, generate msgs, count:", size)
    msgs.extend(newmsgs)
    on_gen_msg = False

def gen_msgs(size):
    dt = datetime.datetime.now() #from now on, one batch , use one time
    dtstr = dt.strftime("%Y-%m-%dT%H:%M:%S")
    acts = ['click', 'view', 'play' ]
    return [{"time":dtstr, "act": acts[random.randint(0,2)], "value":1} for i in range(0,size)]

schedule.every(GEN_BATCH_MSGS_INTERVAL_SEC).seconds.do(gen_msg_job)
schedule.every(SEND_PUBSUB_INTERVAL_SEC).seconds.do(send_job)

while True:
    schedule.run_pending()
    time.sleep(1)