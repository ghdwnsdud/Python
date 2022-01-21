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
df = df[df['MAIN_ITEM_BUILD'].notna()]

df["WIN"] = df.apply(lambda x: 1 if x["WIN"] == "True" else 0 , axis=1)
champ_lst = list(df.groupby("CHAMPIONNAME").count().reset_index()["CHAMPIONNAME"])

df.isnull().sum()

boolean = df["MAIN_ITEM_BUILD"].apply(lambda x: x.count("|") == 2)

boolean

df = df[boolean]

# # 1. 아이템의 키값과 url 매칭

url = "http://ddragon.leagueoflegends.com/cdn/11.24.1/data/ko_KR/item.json"
res = requests.get(url).json()
item_key = list(res['data'].keys())
itemurl = list(map(lambda x: "http://ddragon.leagueoflegends.com/cdn/11.24.1/img/item/"+x+".png", item_key))
item_lst = dict(zip(item_key,itemurl))


# # 2. 각 챔피언마다 메인아이템의 픽률과 승률

def main_item(df):
    champ_games = df.groupby("CHAMPIONNAME")[["GAMEID"]].count().rename(columns = {"GAMEID":"GAMES"}).reset_index()
    main_item_games = df.groupby(["CHAMPIONNAME","MAIN_ITEM_BUILD"])[["WIN"]].count().rename(columns={"WIN":"MAIN_ITEMS_GAMES"}).reset_index()
    main_item_win = df.groupby(["CHAMPIONNAME","MAIN_ITEM_BUILD"])[["WIN"]].sum().reset_index()
    main_item_win_pick = pd.merge(pd.merge(main_item_games, main_item_win, on=["CHAMPIONNAME","MAIN_ITEM_BUILD"]),champ_games, on="CHAMPIONNAME")
    main_item_win_pick["PICK_RATE"] = round(main_item_win_pick.MAIN_ITEMS_GAMES / main_item_win_pick.GAMES * 100,1)
    main_item_win_pick["WIN_RATE"] = round(main_item_win_pick.WIN / main_item_win_pick.MAIN_ITEMS_GAMES * 100,1)
    main_item_win_pick = main_item_win_pick.sort_values("PICK_RATE", ascending=False)

    tmp = []
    for i in champ_lst:
        tmp_lst = []
        tmp_lst.append(main_item_win_pick[main_item_win_pick.CHAMPIONNAME == i].iloc[0]["CHAMPIONNAME"])
        tmp_lst.append(main_item_win_pick[main_item_win_pick.CHAMPIONNAME == i].iloc[0]["MAIN_ITEM_BUILD"])
        tmp_lst.append(main_item_win_pick[main_item_win_pick.CHAMPIONNAME == i].iloc[0]["PICK_RATE"])
        tmp_lst.append(main_item_win_pick[main_item_win_pick.CHAMPIONNAME == i].iloc[0]["WIN_RATE"])

        tmp_lst.append(main_item_win_pick[main_item_win_pick.CHAMPIONNAME == i].iloc[1]["MAIN_ITEM_BUILD"])
        tmp_lst.append(main_item_win_pick[main_item_win_pick.CHAMPIONNAME == i].iloc[1]["PICK_RATE"])
        tmp_lst.append(main_item_win_pick[main_item_win_pick.CHAMPIONNAME == i].iloc[1]["WIN_RATE"])

        tmp_lst.append(main_item_win_pick[main_item_win_pick.CHAMPIONNAME == i].iloc[2]["MAIN_ITEM_BUILD"])
        tmp_lst.append(main_item_win_pick[main_item_win_pick.CHAMPIONNAME == i].iloc[2]["PICK_RATE"])
        tmp_lst.append(main_item_win_pick[main_item_win_pick.CHAMPIONNAME == i].iloc[2]["WIN_RATE"])

        tmp.append(tmp_lst)

    champ_main_item_build = pd.DataFrame(tmp,columns=["CHAMPIONNAME","F_MAIN_ITEM_BUILD","F_PICK_RATE","F_WIN_RATE","S_MAIN_ITEM_BUILD","S_PICK_RATE","S_WIN_RATE"
                                                     ,"T_MAIN_ITEM_BUILD","T_PICK_RATE","T_WIN_RATE"])
    
    champ_main_item_build["MAIN_ITEM_BUILD_IMG"] = champ_main_item_build.apply(lambda x: main_item_img(x), axis=1)
    
    return champ_main_item_build


def main_item_img(row):
    tmp = []
    tmp += list(map(lambda x: item_lst[x], row["F_MAIN_ITEM_BUILD"].split("|")))
    tmp += [str(row.F_PICK_RATE)]
    tmp += [str(row.F_WIN_RATE)]
    
    tmp += list(map(lambda x: item_lst[x], row["S_MAIN_ITEM_BUILD"].split("|")))
    tmp += [str(row.S_PICK_RATE)]
    tmp += [str(row.S_WIN_RATE)]
    
    tmp += list(map(lambda x: item_lst[x], row["T_MAIN_ITEM_BUILD"].split("|")))
    tmp += [str(row.T_PICK_RATE)]
    tmp += [str(row.T_WIN_RATE)]
    
    tmp = "|".join(tmp)
    return tmp


champ_main_item_build = main_item(df)

champ_main_item_build.iloc[0]["MAIN_ITEM_BUILD_IMG"]
