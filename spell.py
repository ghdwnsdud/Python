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

# # 1. 스펠의 키값과 url 매칭

url = "https://ddragon.leagueoflegends.com/cdn/11.24.1/data/ko_KR/summoner.json"
res = requests.get(url).json()
res

res["data"]["Summoner_UltBookSmitePlaceholder"]["key"]

spellId = list(res["data"].keys())

spellkey = list(map(lambda x: res["data"][x]["key"], spellId))

spellurl = list(map(lambda x: "https://ddragon.leagueoflegends.com/cdn/11.24.1/img/spell/"+x+".png", spellId))

spell = dict(zip(spellkey,spellurl))

spell

# # ----------------------------------------------------------------------------------------------------------

# # 2. 각 챔피언마다 스펠의 픽률과 승률

df["WIN"] = df.apply(lambda x: 1 if x["WIN"] == True else 0 , axis=1)


def sepll_lst(row):
    tmp = [row["SPELL1"],row["SPELL2"]]
    tmp.sort(reverse=True)
    tmp = list(map(str,tmp))
    tmp = "|".join(tmp)
    return tmp


df["SEPLL_LST"] = df.apply(lambda x: sepll_lst(x), axis=1)

df

champ_games = df.groupby("CHAMPIONNAME")[["GAMEID"]].count().rename(columns={"GAMEID":"GAMES"}).reset_index()

spell_games = df.groupby(["CHAMPIONNAME","SEPLL_LST"])[["WIN"]].count().rename(columns={"WIN":"SPELL_GAMES"}).reset_index()

spell_games[spell_games.CHAMPIONNAME == "Aatrox"]

spell_win = df.groupby(["CHAMPIONNAME","SEPLL_LST"])[["WIN"]].sum().reset_index()

spell_win[spell_win.CHAMPIONNAME == "Aatrox"]

pd.merge(spell_games,spell_win, on=["CHAMPIONNAME",'SEPLL_LST'])

spell_win_pick = pd.merge(pd.merge(spell_games,spell_win, on=["CHAMPIONNAME",'SEPLL_LST']),champ_games, on="CHAMPIONNAME")

spell_win_pick["PICK_RATE"] = round(spell_win_pick.SPELL_GAMES / spell_win_pick.GAMES * 100,1)

spell_win_pick["WIN_RATE"] = round(spell_win_pick.WIN / spell_win_pick.SPELL_GAMES * 100,1)

spell_win_pick = spell_win_pick.sort_values("PICK_RATE",ascending=False)

spell_win_pick[spell_win_pick.CHAMPIONNAME == "Aatrox"]

champ_lst

tmp = []
for i in champ_lst:
    tmp_lst = []
    tmp_lst.append(spell_win_pick[spell_win_pick.CHAMPIONNAME == i].iloc[0]["CHAMPIONNAME"])
    tmp_lst.append(spell_win_pick[spell_win_pick.CHAMPIONNAME == i].iloc[0]["SEPLL_LST"])
    tmp_lst.append(spell_win_pick[spell_win_pick.CHAMPIONNAME == i].iloc[0]["PICK_RATE"])
    tmp_lst.append(spell_win_pick[spell_win_pick.CHAMPIONNAME == i].iloc[0]["WIN_RATE"])
    
    tmp_lst.append(spell_win_pick[spell_win_pick.CHAMPIONNAME == i].iloc[1]["SEPLL_LST"])
    tmp_lst.append(spell_win_pick[spell_win_pick.CHAMPIONNAME == i].iloc[1]["PICK_RATE"])
    tmp_lst.append(spell_win_pick[spell_win_pick.CHAMPIONNAME == i].iloc[1]["WIN_RATE"])
    
    tmp.append(tmp_lst)

champ_spell = pd.DataFrame(tmp,columns=["CHAMPIONNAME","F_SEPLL","F_PICK_RATE","F_WIN_RATE","S_SEPLL","S_PICK_RATE","S_WIN_RATE"])

champ_spell


def spell_img(row):
    tmp = []
    tmp += list(map(lambda x: spell[x], row["F_SEPLL"].split("|")))
    tmp += [str(row.F_PICK_RATE)]
    tmp += [str(row.F_WIN_RATE)]
    
    tmp += list(map(lambda x: spell[x], row["S_SEPLL"].split("|")))
    tmp += [str(row.S_PICK_RATE)]
    tmp += [str(row.S_WIN_RATE)]
    
    tmp = "|".join(tmp)
    return tmp


champ_spell["SPELL_IMG"] = champ_spell.apply(lambda x: spell_img(x), axis=1)

champ_spell.iloc[0]["SPELL_IMG"]


# # ----------------------------------------------------------------------------------------------------------

# # 3. 위 작업의 함수화

def sepll_lst(row):
    if row["SPELL1"] == 4:
        tmp = [row["SPELL2"],row["SPELL1"]]
    elif row["SPELL2"] == 4:
        tmp = [row["SPELL1"],row["SPELL2"]]
    else:
        tmp = [row["SPELL2"],row["SPELL1"]]
        tmp.sort()
    tmp = list(map(str,tmp))
    tmp = "|".join(tmp)
    return tmp


def spell_img(row):
    tmp = []
    tmp += list(map(lambda x: spell[x], row["F_SEPLL"].split("|")))
    tmp += [str(row.F_PICK_RATE)]
    tmp += [str(row.F_WIN_RATE)]
    
    tmp += list(map(lambda x: spell[x], row["S_SEPLL"].split("|")))
    tmp += [str(row.S_PICK_RATE)]
    tmp += [str(row.S_WIN_RATE)]
    
    tmp = "|".join(tmp)
    return tmp


# +
url = "https://ddragon.leagueoflegends.com/cdn/11.24.1/data/ko_KR/summoner.json"
res = requests.get(url).json()
spellId = list(res["data"].keys())
spellkey = list(map(lambda x: res["data"][x]["key"], spellId))
spellurl = list(map(lambda x: "https://ddragon.leagueoflegends.com/cdn/11.24.1/img/spell/"+x+".png", spellId))
spell = dict(zip(spellkey,spellurl))

#df["WIN"] = df.apply(lambda x: 1 if x["WIN"] == "True" else 0 , axis=1)
df["SEPLL_LST"] = df.apply(lambda x: sepll_lst(x), axis=1)

# +
    champ_games = df.groupby("CHAMPIONNAME")[["GAMEID"]].count().rename(columns={"GAMEID":"GAMES"}).reset_index()
    spell_games = df.groupby(["CHAMPIONNAME","SEPLL_LST"])[["WIN"]].count().rename(columns={"WIN":"SPELL_GAMES"}).reset_index()
    spell_win = df.groupby(["CHAMPIONNAME","SEPLL_LST"])[["WIN"]].sum().reset_index()
    spell_win_pick = pd.merge(pd.merge(spell_games,spell_win, on=["CHAMPIONNAME",'SEPLL_LST']),champ_games, on="CHAMPIONNAME")
    spell_win_pick["PICK_RATE"] = round(spell_win_pick.SPELL_GAMES / spell_win_pick.GAMES * 100,1)
    spell_win_pick["WIN_RATE"] = round(spell_win_pick.WIN / spell_win_pick.SPELL_GAMES * 100,1)
    spell_win_pick = spell_win_pick.sort_values("PICK_RATE",ascending=False)

    tmp = []
    for i in champ_lst:
        tmp_lst = []
        tmp_lst.append(spell_win_pick[spell_win_pick.CHAMPIONNAME == i].iloc[0]["CHAMPIONNAME"])
        tmp_lst.append(spell_win_pick[spell_win_pick.CHAMPIONNAME == i].iloc[0]["SEPLL_LST"])
        tmp_lst.append(spell_win_pick[spell_win_pick.CHAMPIONNAME == i].iloc[0]["PICK_RATE"])
        tmp_lst.append(spell_win_pick[spell_win_pick.CHAMPIONNAME == i].iloc[0]["WIN_RATE"])

        tmp_lst.append(spell_win_pick[spell_win_pick.CHAMPIONNAME == i].iloc[1]["SEPLL_LST"])
        tmp_lst.append(spell_win_pick[spell_win_pick.CHAMPIONNAME == i].iloc[1]["PICK_RATE"])
        tmp_lst.append(spell_win_pick[spell_win_pick.CHAMPIONNAME == i].iloc[1]["WIN_RATE"])

        tmp.append(tmp_lst)

    champ_spell = pd.DataFrame(tmp,columns=["CHAMPIONNAME","F_SEPLL","F_PICK_RATE","F_WIN_RATE","S_SEPLL","S_PICK_RATE","S_WIN_RATE"])

    champ_spell["SPELL_IMG"] = champ_spell.apply(lambda x: spell_img(x), axis=1)


# -

df = df[df['SKILL_BUILD'].notna()]
# 챔피언 리스트 담아두기
champ_lst = list(df.groupby("CHAMPIONNAME").count().reset_index()["CHAMPIONNAME"])

champ_spell = champ_spell_win_pick_rate(df)

champ_spell[champ_spell.CHAMPIONNAME == "Yuumi"]
