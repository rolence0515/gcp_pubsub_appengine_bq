from datetime import timedelta
import datetime
import pandas as pd
import logging

#you can define your time window for min
TIME_WINDOW_SEC = 5*60 #5 min

#you can change your bigquery setting here
DESTINATION_TABLE = 'bqstore.bqstore_2'
PROJECT_ID = 'fluent-opus-185403'
PRIVATE_KEY_FILE_PATH = 'key.json'

#convert string to datetime object
def str_to_dt(dtstr):
    return datetime.datetime.strptime(dtstr, '%Y-%m-%dT%H:%M:%S')

#convert datetime to string
def dt_to_str(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%S')


#round time 
def roundTime(dt=None, roundTo=60):
    if dt == None : dt = datetime.datetime.now()
    seconds = (dt.replace(tzinfo=None) - dt.min).seconds
    rounding = (seconds+roundTo/2) // roundTo * roundTo
    return dt + datetime.timedelta(0,rounding-seconds,-dt.microsecond)


#add round field
def add_round_time_field(item):
    item["round_time"] =  roundTime(str_to_dt(item["time"]), TIME_WINDOW_SEC)
    return item

#main exporsed method
def load_msg_to_bigquery(msgs):
    msgs2 = list( map(add_round_time_field, msgs))
    df = pd.DataFrame.from_dict(msgs)
    df2 = df.groupby(['round_time', 'act'])["value"].sum().unstack()
    df2.reset_index(level=0, inplace=True)

    dfstr = df2.to_string()
    logging.info(dfstr)

    pd.io.gbq.to_gbq(df2, DESTINATION_TABLE, PROJECT_ID, if_exists="append", private_key = PRIVATE_KEY_FILE_PATH )