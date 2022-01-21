# -*- coding: utf-8 -*-
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
import final_df as fd
tqdm.pandas()

dhb.db_open()
df = dhb.sql("select * from match_aram")
dhb.db_close()

df[df.BOOTS == 0]

test = df.groupby(["GAMEID"])[['PARTICIPANTID']].count()
test[test.PARTICIPANTID != 10]

df = pd.read_csv("725700.csv")

# # . 최종DF 컬럼에 넣기

final_df = fd.final_df(df)
final_df = final_df[["CHAMPIONNAME","RUNE_IMG_ONE","RUNE_IMG_TWO","SPELL_IMG","STARTING_ITEM_IMG_ONE","STARTING_ITEM_IMG_TWO","MAIN_ITEM_BUILD_IMG",
                     "BOOTS_IMG","SKILL_IMG","SKILL_KEY","SYNERGY_TIER","COMBINATION_SCORE","K_SYNERGY"]].T.drop_duplicates().T

final_df.columns

final_df


def champ_img(name):
    champion_lst = []
    url = 'http://ddragon.leagueoflegends.com/cdn/11.23.1/img/champion/'+name+'.png'
    #champion_lst.append(name)
    champion_lst.append(res["data"][name]["name"])
    champion_lst.append(res["data"][name]["id"])
    champion_lst.append(url)
    return champion_lst


url = "https://ddragon.leagueoflegends.com/cdn/11.24.1/data/ko_KR/champion.json"
res = requests.get(url).json()
champ_lst = list(res["data"].keys())
champ_key = list(map(lambda x: res["data"][x]["key"], champ_lst))
champion = dict(list(zip(champ_key, champ_lst)))
tmp = list(map(lambda x: champ_img(x), champ_lst))
champion = dict(list(zip(champ_key, tmp)))

test = pd.DataFrame(champion).T
test = test.rename(columns={0:"CHAMPNAME",1:"CHAMPIONNAME",2:"CHAMP_IMG"})
test.reset_index(inplace=True)
test = test.rename(columns={"index":"CHAMP_CODE"})
test[test.CHAMPIONNAME =="Fiddlesticks"] = test[test.CHAMPIONNAME =="Fiddlesticks"].replace("Fiddlesticks","FiddleSticks")
testing = pd.merge(test, final_df, on="CHAMPIONNAME")

test.to_csv("CHAMP_LST.csv",header=True, index=False, encoding="utf-8-sig")

champ_tier = pd.read_csv("champ_tier.csv")
champ_tier = champ_tier.rename(columns={"CHAMPIONNAME":"CHAMPNAME"})

champ_tier

testing = pd.merge(testing, champ_tier, on="CHAMPNAME")

testing.to_csv("CHAMPINFO.csv",header=True, index=False, encoding="utf-8-sig")

testing
