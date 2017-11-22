from flask import Flask, render_template
import pandas as pd
from pandas.io import gbq
import plotly.plotly as py
import plotly.graph_objs as go
from plotly.tools import FigureFactory as FF
import plotly
import json

KEYPATH = "key.json"
PROJECT_ID = "fluent-opus-185403" 
PLOTLY_USER = 'reddoorgcpbigtable'
PLOTLY_KEY = 'Sw2ESURp11VO3geqaz7O'

def query_bq_to_dataframe():
    group_5_min_query = """
    SELECT round_time, sum(click) as click, sum(view) as view, sum(play) as play
    FROM [fluent-opus-185403:bqstore.bqstore_2] 
    WHERE _PARTITIONTIME >= "2017-11-15 00:00:00" AND _PARTITIONTIME < "2017-11-30 00:00:00"
    GROUP BY round_time
    ORDER BY round_time
    """        
    df = pd.io.gbq.read_gbq(group_5_min_query, project_id=PROJECT_ID, private_key= KEYPATH)
    plotly.tools.set_credentials_file(username=PLOTLY_USER, api_key=PLOTLY_KEY)
    return df



