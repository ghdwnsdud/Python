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
tqdm.pandas()

dhb.db_open()
df = dhb.sql("select * from match_aram")
dhb.db_close()

# # . 신발 픽률

df["WIN"] = df.apply(lambda x: 1 if x["WIN"] == "True" else 0 , axis=1)
champ_lst = list(df.groupby("CHAMPIONNAME").count().reset_index()["CHAMPIONNAME"])
url = "http://ddragon.leagueoflegends.com/cdn/11.24.1/data/ko_KR/item.json"
res = requests.get(url).json()
item_key = list(res['data'].keys())
itemurl = list(map(lambda x: "http://ddragon.leagueoflegends.com/cdn/11.24.1/img/item/"+x+".png", item_key))
item_lst = dict(zip(item_key,itemurl))


def boots_img(row):
    if row.CHAMPIONNAME != "Cassiopeia":
        tmp = []
        try:
            tmp += list(map(lambda x: item_lst[x], row["F_BOOTS"].split("|")))
        except:
            tmp += ["NoShoes"]
            
        tmp += [str(row.F_PICK_RATE)]
        tmp += [str(row.F_WIN_RATE)]
        
        tmp += list(map(lambda x: item_lst[x], row["S_BOOTS"].split("|")))
        tmp += [str(row.S_PICK_RATE)]
        tmp += [str(row.S_WIN_RATE)]
       
        tmp = "|".join(tmp)
    else:
        tmp = "0"
    return tmp


def boots_build(df):   
    champ_games = df.groupby("CHAMPIONNAME")[["GAMEID"]].count().rename(columns = {"GAMEID":"GAMES"}).reset_index()
    boots_games = df.groupby(["CHAMPIONNAME","BOOTS"])[["WIN"]].count().rename(columns={"WIN":"BOOTS_GAMES"}).reset_index()
    boots_win = df.groupby(["CHAMPIONNAME","BOOTS"])[["WIN"]].sum().reset_index()
    boots_win_pick = pd.merge(pd.merge(boots_games, boots_win, on=["CHAMPIONNAME","BOOTS"]),champ_games, on="CHAMPIONNAME")
    boots_win_pick["PICK_RATE"] = round(boots_win_pick.BOOTS_GAMES / boots_win_pick.GAMES * 100,1)
    boots_win_pick["WIN_RATE"] = round(boots_win_pick.WIN / boots_win_pick.BOOTS_GAMES * 100,1)
    boots_win_pick = boots_win_pick.sort_values("PICK_RATE", ascending=False)

    tmp = []
    for i in champ_lst:
        if len(boots_win_pick[boots_win_pick.CHAMPIONNAME == i]) > 1:
            tmp_lst = []
            tmp_lst.append(boots_win_pick[boots_win_pick.CHAMPIONNAME == i].iloc[0]["CHAMPIONNAME"])
            tmp_lst.append(boots_win_pick[boots_win_pick.CHAMPIONNAME == i].iloc[0]["BOOTS"])
            tmp_lst.append(boots_win_pick[boots_win_pick.CHAMPIONNAME == i].iloc[0]["PICK_RATE"])
            tmp_lst.append(boots_win_pick[boots_win_pick.CHAMPIONNAME == i].iloc[0]["WIN_RATE"])

            if boots_win_pick[boots_win_pick.CHAMPIONNAME == i].iloc[1]["BOOTS"] != "0":
                tmp_lst.append(boots_win_pick[boots_win_pick.CHAMPIONNAME == i].iloc[1]["BOOTS"])
                tmp_lst.append(boots_win_pick[boots_win_pick.CHAMPIONNAME == i].iloc[1]["PICK_RATE"])
                tmp_lst.append(boots_win_pick[boots_win_pick.CHAMPIONNAME == i].iloc[1]["WIN_RATE"])
            else:
                tmp_lst.append(boots_win_pick[boots_win_pick.CHAMPIONNAME == i].iloc[2]["BOOTS"])
                tmp_lst.append(boots_win_pick[boots_win_pick.CHAMPIONNAME == i].iloc[2]["PICK_RATE"])
                tmp_lst.append(boots_win_pick[boots_win_pick.CHAMPIONNAME == i].iloc[2]["WIN_RATE"])

            tmp.append(tmp_lst)
        else:
            tmp_lst = []
            tmp_lst.append(boots_win_pick[boots_win_pick.CHAMPIONNAME == i].iloc[0]["CHAMPIONNAME"])
            tmp_lst.append("0")
            tmp_lst.append("0")
            tmp_lst.append("0")
            tmp_lst.append("0")
            tmp_lst.append("0")
            tmp_lst.append("0")
            tmp.append(tmp_lst)

    champ_boots = pd.DataFrame(tmp,columns=["CHAMPIONNAME","F_BOOTS","F_PICK_RATE","F_WIN_RATE","S_BOOTS","S_PICK_RATE","S_WIN_RATE"])

    champ_boots["BOOTS_IMG"] = champ_boots.apply(lambda x: boots_img(x), axis=1)
    
    return champ_boots


champ_boots = boots_build(df)

champ_boots
