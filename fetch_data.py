"""
import pandas as pd
import os.path
import matplotlib.pyplot as pyplot
import seaborn as sns
import re
from IPython.display import display
# %matplotlib inline
from bs4 import BeautifulSoup
"""
import requests
import json
import urllib.request
url = 'http://rbi.ddns.net/getBreadCrumbData'
datafile = '/home/shared_dir/assignment_1/examples/clients/cloud/python/datafile.json'
urllib.request.urlretrieve(url,datafile) 

slack_uri = 'https://hooks.slack.com/services/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'

def test_site(slack_uri):
    success_dict = { "text": "data updated" }
    success_json = json.dumps(success_dict)
    headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
    requests.post(slack_uri, data=success_json, headers=headers)

test_site(slack_uri)

