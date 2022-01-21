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

df["WIN"] = df.apply(lambda x: 1 if x["WIN"] == "True" else 0 , axis=1)

champ_lst = list(df.groupby("CHAMPIONNAME").count().reset_index()["CHAMPIONNAME"])

# # 1. 아이템의 키값과 url 매칭

url = "http://ddragon.leagueoflegends.com/cdn/11.24.1/data/ko_KR/item.json"
res = requests.get(url).json()
item_key = list(res['data'].keys())

list(map(lambda x: {x:"http://ddragon.leagueoflegends.com/cdn/11.24.1/img/item/"+x+".png"}, item_key))

itemurl = list(map(lambda x: "http://ddragon.leagueoflegends.com/cdn/11.24.1/img/item/"+x+".png", item_key))

item_lst = dict(zip(item_key,itemurl))

item_lst

# # ----------------------------------------------------------------------------------------------------------

# # 2. 각 챔피언마다 시작아이템의 픽률과 승률

champ_games = df.groupby("CHAMPIONNAME")[["GAMEID"]].count().rename(columns = {"GAMEID":"GAMES"}).reset_index()

starting_item_games = df.groupby(["CHAMPIONNAME","STARTING_ITEM"])[["WIN"]].count().rename(columns={"WIN":"STARTING_ITEMS_GAMES"}).reset_index()

starting_item_games

starting_item_win = df.groupby(["CHAMPIONNAME","STARTING_ITEM"])[["WIN"]].sum().reset_index()

starting_item_win

starting_item_win_pick = pd.merge(pd.merge(starting_item_games, starting_item_win, on=["CHAMPIONNAME","STARTING_ITEM"]),champ_games, on="CHAMPIONNAME")

starting_item_win_pick

starting_item_win_pick["PICK_RATE"] = round(starting_item_win_pick.STARTING_ITEMS_GAMES / starting_item_win_pick.GAMES * 100,1)

starting_item_win_pick["WIN_RATE"] = round(starting_item_win_pick.WIN / starting_item_win_pick.STARTING_ITEMS_GAMES * 100,1)

starting_item_win_pick = starting_item_win_pick.sort_values("PICK_RATE", ascending=False)

starting_item_win_pick[starting_item_win_pick.CHAMPIONNAME == "Aatrox"]

tmp = []
for i in champ_lst:
    tmp_lst = []
    tmp_lst.append(starting_item_win_pick[starting_item_win_pick.CHAMPIONNAME == i].iloc[0]["CHAMPIONNAME"])
    tmp_lst.append(starting_item_win_pick[starting_item_win_pick.CHAMPIONNAME == i].iloc[0]["STARTING_ITEM"])
    tmp_lst.append(starting_item_win_pick[starting_item_win_pick.CHAMPIONNAME == i].iloc[0]["PICK_RATE"])
    tmp_lst.append(starting_item_win_pick[starting_item_win_pick.CHAMPIONNAME == i].iloc[0]["WIN_RATE"])
    
    tmp_lst.append(starting_item_win_pick[starting_item_win_pick.CHAMPIONNAME == i].iloc[1]["STARTING_ITEM"])
    tmp_lst.append(starting_item_win_pick[starting_item_win_pick.CHAMPIONNAME == i].iloc[1]["PICK_RATE"])
    tmp_lst.append(starting_item_win_pick[starting_item_win_pick.CHAMPIONNAME == i].iloc[1]["WIN_RATE"])
    
    tmp.append(tmp_lst)

champ_starting_item = pd.DataFrame(tmp,columns=["CHAMPIONNAME","F_STARTING_ITEM","F_PICK_RATE","F_WIN_RATE","S_STARTING_ITEM","S_PICK_RATE","S_WIN_RATE"])

champ_starting_item


def starting_item_img(row):
    tmp = []
    tmp += list(map(lambda x: item_lst[x], row["F_STARTING_ITEM"].split("|")))
    tmp += [str(row.F_PICK_RATE)]
    tmp += [str(row.F_WIN_RATE)]
    
    tmp += list(map(lambda x: item_lst[x], row["S_STARTING_ITEM"].split("|")))
    tmp += [str(row.S_PICK_RATE)]
    tmp += [str(row.S_WIN_RATE)]
    
    tmp = "|".join(tmp)
    return tmp


champ_starting_item["STARTING_ITEM_IMG"] = champ_starting_item.apply(lambda x: starting_item_img(x), axis=1)

champ_starting_item.iloc[105]["STARTING_ITEM_IMG"].split("|")


# # 3. 위 작업의 함수화

def starting_item_img_one(row):
    tmp = []
    tmp += list(map(lambda x: item_lst[x], row["F_STARTING_ITEM"].split("|")))
    tmp += [str(row.F_PICK_RATE)]
    tmp += [str(row.F_WIN_RATE)]
    
    tmp = "|".join(tmp)
    return tmp


def starting_item_img_two(row):
    tmp = []
    tmp += list(map(lambda x: item_lst[x], row["S_STARTING_ITEM"].split("|")))
    tmp += [str(row.S_PICK_RATE)]
    tmp += [str(row.S_WIN_RATE)]
    
    tmp = "|".join(tmp)
    return tmp


url = "http://ddragon.leagueoflegends.com/cdn/11.24.1/data/ko_KR/item.json"
res = requests.get(url).json()
item_key = list(res['data'].keys())
itemurl = list(map(lambda x: "http://ddragon.leagueoflegends.com/cdn/11.24.1/img/item/"+x+".png", item_key))
item_lst = dict(zip(item_key,itemurl))


def starting_item(df):
    champ_games = df.groupby("CHAMPIONNAME")[["GAMEID"]].count().rename(columns = {"GAMEID":"GAMES"}).reset_index()
    starting_item_games = df.groupby(["CHAMPIONNAME","STARTING_ITEM"])[["WIN"]].count().rename(columns={"WIN":"STARTING_ITEMS_GAMES"}).reset_index()
    starting_item_win = df.groupby(["CHAMPIONNAME","STARTING_ITEM"])[["WIN"]].sum().reset_index()
    starting_item_win_pick = pd.merge(pd.merge(starting_item_games, starting_item_win, on=["CHAMPIONNAME","STARTING_ITEM"]),champ_games, on="CHAMPIONNAME")
    starting_item_win_pick["PICK_RATE"] = round(starting_item_win_pick.STARTING_ITEMS_GAMES / starting_item_win_pick.GAMES * 100,1)
    starting_item_win_pick["WIN_RATE"] = round(starting_item_win_pick.WIN / starting_item_win_pick.STARTING_ITEMS_GAMES * 100,1)
    starting_item_win_pick = starting_item_win_pick.sort_values("PICK_RATE", ascending=False)
    
    tmp = []
    for i in champ_lst:
        tmp_lst = []
        tmp_lst.append(starting_item_win_pick[starting_item_win_pick.CHAMPIONNAME == i].iloc[0]["CHAMPIONNAME"])
        tmp_lst.append(starting_item_win_pick[starting_item_win_pick.CHAMPIONNAME == i].iloc[0]["STARTING_ITEM"])
        tmp_lst.append(starting_item_win_pick[starting_item_win_pick.CHAMPIONNAME == i].iloc[0]["PICK_RATE"])
        tmp_lst.append(starting_item_win_pick[starting_item_win_pick.CHAMPIONNAME == i].iloc[0]["WIN_RATE"])

        tmp_lst.append(starting_item_win_pick[starting_item_win_pick.CHAMPIONNAME == i].iloc[1]["STARTING_ITEM"])
        tmp_lst.append(starting_item_win_pick[starting_item_win_pick.CHAMPIONNAME == i].iloc[1]["PICK_RATE"])
        tmp_lst.append(starting_item_win_pick[starting_item_win_pick.CHAMPIONNAME == i].iloc[1]["WIN_RATE"])

        tmp.append(tmp_lst)
        
    champ_starting_item = pd.DataFrame(tmp,columns=["CHAMPIONNAME","F_STARTING_ITEM","F_PICK_RATE","F_WIN_RATE","S_STARTING_ITEM","S_PICK_RATE","S_WIN_RATE"])

    champ_starting_item["STARTING_ITEM_IMG_ONE"] = champ_starting_item.apply(lambda x: starting_item_img_one(x), axis=1)
    
    champ_starting_item["STARTING_ITEM_IMG_TWO"] = champ_starting_item.apply(lambda x: starting_item_img_two(x), axis=1)

    return champ_starting_item


champ_starting_item = starting_item(df)

champ_starting_item
