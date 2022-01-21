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
df = df[df['SKILL_BUILD'].notna()]

df

df[df.CHAMPIONNAME == "Kaisa"]

df.columns


def delete(row):
    query = (
    f' delete from match_aram where gameid=\'{row.GAMEID}\''
    )
    dhb.sql(query)
    return


delete_gameid = df.groupby(["GAMEID"])[['PARTICIPANTID']].count()
delete_gameid = delete_gameid[delete_gameid.PARTICIPANTID != 10].reset_index()
delete_gameid

dhb.db_open()
delete_gameid.apply(lambda x: delete(x), axis=1)
dhb.db_close()

df = pd.read_csv("sample.csv")

df["WIN"] = df.apply(lambda x: 1 if x["WIN"] == True else 0 , axis=1)

df = df[df['SKILL_BUILD'].notna()]

dhb.db_close()

# # . Skill_Build 부분 수정

champ_lst = list(df.groupby("CHAMPIONNAME").count().reset_index()["CHAMPIONNAME"])

url ="https://ddragon.leagueoflegends.com/cdn/11.24.1/data/ko_KR/champion.json"
res = requests.get(url).json()
champion = list(res["data"].keys())
url = "https://ddragon.leagueoflegends.com/cdn/11.24.1/data/ko_KR/champion/Aatrox.json"
res = requests.get(url).json()
champ_json = list(map(lambda x: "https://ddragon.leagueoflegends.com/cdn/11.24.1/data/ko_KR/champion/"+x+".json", champion))
champ_skill_img = list(map(lambda x: champ_skill(x), champion))
champ_skill_img = dict(zip(champ_lst,champ_skill_img))


def skill(df):
    champ_games = df.groupby(["CHAMPIONID","CHAMPIONNAME"])[["GAMEID"]].count().rename(columns = {"GAMEID":"GAMES"}).reset_index()
    df["SKILL_BUILD"] = df.apply(lambda x: x.SKILL_BUILD.split("|"), axis=1)
    df["SKILL_BUILD"] = df.apply(lambda x: skill_tree(x), axis=1)
    skill_games = df.groupby(["CHAMPIONID","CHAMPIONNAME","SKILL_BUILD"])[["WIN"]].count().rename(columns={"WIN":"SKILL_GAMES"}).reset_index()
    skill_win = df.groupby(["CHAMPIONID","CHAMPIONNAME","SKILL_BUILD"])[["WIN"]].sum().reset_index()
    skill_win_rate = pd.merge(pd.merge(skill_games, skill_win, on=["CHAMPIONID","CHAMPIONNAME","SKILL_BUILD"]),champ_games,on=["CHAMPIONID","CHAMPIONNAME"])
    skill_win_rate["PICK_RATE"] = round(skill_win_rate.SKILL_GAMES/skill_win_rate.GAMES *100 , 1)
    skill_win_rate["WIN_RATE"] = round(skill_win_rate.WIN/skill_win_rate.SKILL_GAMES *100 , 1)
    skill_win_rate = skill_win_rate.sort_values("PICK_RATE", ascending=False)

    tmp = []
    for i in champ_lst:
        tmp_lst = []
        tmp_lst.append(skill_win_rate[skill_win_rate.CHAMPIONNAME == i].iloc[0]["CHAMPIONID"])
        tmp_lst.append(skill_win_rate[skill_win_rate.CHAMPIONNAME == i].iloc[0]["CHAMPIONNAME"])
        tmp_lst.append(skill_win_rate[skill_win_rate.CHAMPIONNAME == i].iloc[0]["SKILL_BUILD"])
        tmp_lst.append(skill_win_rate[skill_win_rate.CHAMPIONNAME == i].iloc[0]["PICK_RATE"])
        tmp_lst.append(skill_win_rate[skill_win_rate.CHAMPIONNAME == i].iloc[0]["WIN_RATE"])

        tmp.append(tmp_lst)

    champ_skill_build = pd.DataFrame(tmp,columns=["CHAMPIONID","CHAMPIONNAME","SKILL_BUILD","PICK_RATE","WIN_RATE"])

    champ_skill_build["SKILL_IMG"] = champ_skill_build.apply(lambda x: skill_img(x), axis=1)

    return champ_skill_build


def skill_tree(row):
    remove_set = {"4"}
    skill = row.SKILL_BUILD
    result = [i for i in skill if i not in remove_set]
    skill_1 = 0
    skill_2 = 0
    skill_3 = 0
    skill_build = []
    for i in result:
        if i == '1':
            skill_1 += 1
        elif i == '2':
            skill_2 += 1
        elif i == '3':
            skill_3 += 1

        if skill_1 == 5:
            if i in skill_build:
                continue
            skill_build.append(i)
        elif skill_2 == 5:
            if i in skill_build:
                continue
            skill_build.append(i)
        elif skill_3 == 5:
            if i in skill_build:
                continue
            skill_build.append(i)
            
    if 2 == len(skill_build):
        if "1" not in skill_build:
            skill_build.append("1")
        elif "2" not in skill_build:
            skill_build.append("2")
        elif "3" not in skill_build:
            skill_build.append("3")
            
    return "|".join(skill_build)


# # . -----------------------------------------------------------------------------------------------------------

def champ_skill(x):
    url = "https://ddragon.leagueoflegends.com/cdn/11.24.1/data/ko_KR/champion/"+x+".json"
    res = requests.get(url).json()
    Q = "http://ddragon.leagueoflegends.com/cdn/11.24.1/img/spell/"+res["data"][x]["spells"][0]["id"]+".png"
    W = "http://ddragon.leagueoflegends.com/cdn/11.24.1/img/spell/"+res["data"][x]["spells"][1]["id"]+".png"
    E = "http://ddragon.leagueoflegends.com/cdn/11.24.1/img/spell/"+res["data"][x]["spells"][2]["id"]+".png"
    
    champ_qwe = {"1":Q, "2":W, "3":E}
    
    return champ_qwe


def skill_img(row):
    tmp = []
    tmp += list(map(lambda x: champ_skill_img[row.CHAMPIONNAME][x], row["SKILL_BUILD"].split("|")))
    tmp += [str(row.PICK_RATE)]
    tmp += [str(row.WIN_RATE)]
    
    tmp = "|".join(tmp)
    
    return tmp


champ_skill_build = skill(df)

skill_key = {"1":"Q","2":"W","3":"E"} 


def skill_k(row):
    skill_key = {"1":"Q","2":"W","3":"E"} 
    tmp = []
    tmp = list(map(lambda x: skill_key[x], row.SKILL_BUILD.split("|")))
    tmp = "|".join(tmp)
    return tmp


champ_skill_build["SKILL_KEY"] = champ_skill_build.apply(lambda x: skill_k(x), axis=1)

champ_skill_build

champ_skill_img
