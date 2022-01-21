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
import my_utils as mu
import pandas as pd
import requests
import time
import DHB as dhb
tqdm.pandas()

df = pd.read_csv("sample.csv")

dhb.db_open()
df = dhb.sql("select * from match_aram")
dhb.db_close()

df.columns

# # . 팀 조합

df["WIN"] = df.apply(lambda x: 1 if x["WIN"] == True else 0 , axis=1)

# +
# 팀 조합승률 구하는 함수
games = df[['CHAMPIONNAME','WIN']].groupby(["CHAMPIONNAME"]).count().rename(columns={"WIN":"GAMES"})
games_win = df[['CHAMPIONNAME','WIN']].groupby(["CHAMPIONNAME"]).sum()
win_rate = pd.merge(games, games_win, on=["CHAMPIONNAME"])
win_rate["rate"] = round(win_rate.WIN / win_rate.GAMES * 100,1)

blue_df = df[df.TEAMID == 100]
red_df = df[df.TEAMID == 200]

blue_test = pd.merge(blue_df,blue_df[["GAMEID","CHAMPIONNAME"]], on=["GAMEID"])
blue_test = blue_test.rename(columns={'CHAMPIONNAME_x':"CHAMPIONNAME","CHAMPIONNAME_y":"TEAMCHAMPION"})
blue_test2 = blue_test[["CHAMPIONNAME",'TEAMCHAMPION',"WIN"]].groupby(["CHAMPIONNAME",'TEAMCHAMPION']).count().rename(columns={"WIN":"games"})
blue_test2 = blue_test2.reset_index()
blue_test2 = blue_test2[blue_test2.CHAMPIONNAME != blue_test2.TEAMCHAMPION]
blue_test2_win = blue_test[["CHAMPIONNAME",'TEAMCHAMPION',"WIN"]].groupby(["CHAMPIONNAME",'TEAMCHAMPION']).sum().reset_index()
blue_test2_win = blue_test2_win[blue_test2_win.CHAMPIONNAME != blue_test2_win.TEAMCHAMPION]
blue_champ_combination = pd.merge(blue_test2,blue_test2_win, on=["CHAMPIONNAME",'TEAMCHAMPION'])

red_test = pd.merge(red_df,red_df[["GAMEID","CHAMPIONNAME"]], on=["GAMEID"])
red_test = red_test.rename(columns={'CHAMPIONNAME_x':"CHAMPIONNAME","CHAMPIONNAME_y":"TEAMCHAMPION"})
red_test2 = red_test[["CHAMPIONNAME",'TEAMCHAMPION',"WIN"]].groupby(["CHAMPIONNAME",'TEAMCHAMPION']).count().rename(columns={"WIN":"games"})
red_test2 = red_test2.reset_index()
red_test2 = red_test2[red_test2.CHAMPIONNAME != red_test2.TEAMCHAMPION]
red_test2_win = red_test[["CHAMPIONNAME",'TEAMCHAMPION',"WIN"]].groupby(["CHAMPIONNAME",'TEAMCHAMPION']).sum().reset_index()
red_test2_win = red_test2_win[red_test2_win.CHAMPIONNAME != red_test2_win.TEAMCHAMPION]
red_champ_combination = pd.merge(red_test2,red_test2_win, on=["CHAMPIONNAME",'TEAMCHAMPION'])

champ_combination = blue_champ_combination.append(red_champ_combination).groupby(["CHAMPIONNAME",'TEAMCHAMPION']).sum().reset_index()
champ_combination["RATE"] = round(champ_combination.WIN / champ_combination.games * 100 ,1)
combination_rate = pd.merge(win_rate,champ_combination, on=["CHAMPIONNAME"]).drop(['WIN_x','WIN_y'],axis=1)\
                                                        .rename(columns={"rate":"default_rate","RATE":"combination_rate"})
combination_rate["COMBINATION_SCORE"] = combination_rate.combination_rate - combination_rate.default_rate

combination_rate["rank"] = combination_rate.apply(lambda x : rank(x), axis=1)
#     combination_rate[combination_rate.TEAMCHAMPION =="FiddleSticks"] = combination_rate[combination_rate.TEAMCHAMPION 
#                                                                                         =="FiddleSticks"].replace("FiddleSticks","Fiddlesticks")
combination_rate["SYNERGY_TIER"] = combination_rate.apply(lambda x: x.TEAMCHAMPION, axis=1)
combination_rate = combination_rate[["CHAMPIONNAME","TEAMCHAMPION","COMBINATION_SCORE","rank","SYNERGY_TIER"]]

champ_lst = pd.read_csv("CHAMP_LST.csv")
champ_lst = champ_lst[["CHAMPIONNAME","CHAMPNAME"]]
champ_lst = champ_lst.rename(columns={"CHAMPIONNAME":"SYNERGY_TIER"})

cr2 = pd.merge(combination_rate, champ_lst, on="SYNERGY_TIER").sort_values("COMBINATION_SCORE", ascending=False)
cr2["COMBINATION_SCORE"] = cr2.apply(lambda x: score_str(x), axis=1)

# combination_rate.sort_values("COMBINATION_SCORE",ascending=False, inplace=True)

# combination_rate["COMBINATION_SCORE"] = combination_rate.apply(lambda x: score_str(x), axis=1)


combination_rate_img = cr2.groupby(["CHAMPIONNAME","rank"])["SYNERGY_TIER"].apply(lambda x: "%s" % '|'.join(x))
combination_rate_combination_score = cr2.groupby(["CHAMPIONNAME","rank"])["COMBINATION_SCORE"].apply(lambda x: "%s" % '|'.join(x))
combination_rate_ko_name = cr2.groupby(["CHAMPIONNAME","rank"])["CHAMPNAME"].apply(lambda x: "%s" % '|'.join(x))


combination_rate_img = pd.DataFrame(combination_rate_img)
combination_rate_img.reset_index(inplace=True)
combination_rate_img = combination_rate_img.groupby(["CHAMPIONNAME"])["SYNERGY_TIER"].apply(lambda x: "%s" % ','.join(x))

combination_rate_combination_score = pd.DataFrame(combination_rate_combination_score)
combination_rate_combination_score.reset_index(inplace=True)
combination_rate_combination_score = combination_rate_combination_score.groupby(["CHAMPIONNAME"])["COMBINATION_SCORE"].apply(lambda x: "%s" % 
                                                                                                                             ','.join(x))

combination_rate_ko_name = pd.DataFrame(combination_rate_ko_name)
combination_rate_ko_name.reset_index(inplace=True)
combination_rate_ko_name = combination_rate_ko_name.groupby(["CHAMPIONNAME"])["CHAMPNAME"].apply(lambda x: "%s" % ','.join(x))


combination_rate_img = pd.DataFrame(combination_rate_img)
combination_rate_combination_score = pd.DataFrame(combination_rate_combination_score)
combination_rate_ko_name = pd.DataFrame(combination_rate_ko_name)

synergy1 = pd.merge(combination_rate_img,combination_rate_combination_score, on=["CHAMPIONNAME"]).reset_index()
synergy = pd.merge(synergy1, combination_rate_ko_name ,on="CHAMPIONNAME")
# -

combination_rate_ko_name.loc["Aatrox"]["CHAMPNAME"].split(",")[0].split("|")

synergy1 = pd.merge(synergy, combination_rate_ko_name ,on="CHAMPIONNAME").rename(columns={"CHAMPNAME":"K_SYNERGY"})

synergy.iloc[137]["SYNERGY_TIER"].split(",")[0].split("|")

synergy.iloc[137]["CHAMPNAME"].split(",")[0].split("|")

synergy = combination(df)

champ_lst = pd.read_csv("CHAMP_LST.csv")
champ_lst = champ_lst[["CHAMPIONNAME","CHAMPNAME"]]
champ_lst = champ_lst.rename(columns={"CHAMPIONNAME":"SYNERGY_TIER"})
combination_rate = pd.merge(combination_rate, champ_lst, on="SYNERGY_TIER")

combination_rate_ko_name = combination_rate.groupby(["CHAMPIONNAME","rank"])["CHAMPNAME"].apply(lambda x: "%s" % '|'.join(x))


# ## -----------------------------------------------------------------------------------------------------------------------------

def rank(row):
    if 20 > row.COMBINATION_SCORE >= 5:
        return "A"
    elif 5 > row.COMBINATION_SCORE > -5:
        return "C"
    elif -30 < row.COMBINATION_SCORE < -5:
        return "B"


def score_str(row):
    a = "{:.1f}%".format(row.COMBINATION_SCORE)
    return a


# 최종적으로 스프링단에 표현할때 아래와 같이 써주면 될 것 같다.
A = testing3.iloc[0]["img"].split(",")[0].split("|")
A_RATE = testing3.iloc[0]["combination_score"].split(",")[0].split("|")
B = testing3.iloc[0]["img"].split(",")[1].split("|")
B_RATE = testing3.iloc[0]["combination_score"].split(",")[1].split("|")
