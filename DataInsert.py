# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.13.6
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

from random import sample
import random
from tqdm import tqdm
import pandas as pd
import requests
import time
import DHB as dhb
tqdm.pandas()

for i in range(50):
    raw_data = dhb.get_aram()
    if type(raw_data) != "filter":
        df = dhb.preprocessing(raw_data)
        dhb.db_open()
        dhb.insert_match(df)
        dhb.db_close()
        time.sleep(90)
    else:
        time.sleep(90)
