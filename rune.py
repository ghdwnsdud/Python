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

df

df.columns

df = df.groupby(["GAMEID"])[['PARTICIPANTID']].count()

df[df.PARTICIPANTID != 10]

df.isnull().sum()

df["WIN"] = df.apply(lambda x: 1 if x["WIN"] == "True" else 0 , axis=1)


# # 1.

def rune_img(row):
    tmp = []
    tmp += list(map(lambda x: 'https://ddragon.leagueoflegends.com/cdn/img/'+rune_lst[x], row["MAIN_RUNE"].split("|")))
    tmp += list(map(lambda x: 'https://ddragon.leagueoflegends.com/cdn/img/'+rune_lst[x], row["SUB_RUNE"].split("|")))
    tmp += list(map(lambda x: stat_rune[x], row["STAT_RUNE"].split("|")))
    tmp += [str(row.RATE)]
    
    tmp = "|".join(tmp)
    return tmp


# +
# 먼저 메인룬만 뽑아옴
url = "https://ddragon.leagueoflegends.com/cdn/11.24.1/data/ko_KR/runesReforged.json"
res = requests.get(url).json()
main_rune = list(map(lambda x: ({str(x["id"]):x["icon"]}), res))
main_rune = {**main_rune[0],**main_rune[1],**main_rune[2],**main_rune[3],**main_rune[4]}

# 서브룬을 만들어 줌
sub_rune = list(map(lambda x: list(map(lambda z: list(map(lambda c: ({str(c["id"]):c["icon"]}), z["runes"])), x["slots"])), res))

# 3차원배열이여서 1차원으로 축소
sub_rune = [element for array in sub_rune for element in array]
sub_rune = [element for array in sub_rune for element in array]

# key값으로 검색할 수 있게 하나의 딕셔너리로 만드는 과정
sub_rune_lst = {**sub_rune[0],**sub_rune[1],**sub_rune[2],**sub_rune[3],**sub_rune[4],**sub_rune[5],**sub_rune[6]
               ,**sub_rune[7],**sub_rune[8],**sub_rune[9],**sub_rune[10],**sub_rune[11],**sub_rune[12],**sub_rune[13]
               ,**sub_rune[14],**sub_rune[15],**sub_rune[16],**sub_rune[17],**sub_rune[18],**sub_rune[19],**sub_rune[20]
               ,**sub_rune[21],**sub_rune[22],**sub_rune[23],**sub_rune[24],**sub_rune[25],**sub_rune[26],**sub_rune[27]
               ,**sub_rune[28],**sub_rune[29],**sub_rune[30],**sub_rune[31],**sub_rune[32],**sub_rune[33],**sub_rune[34]
               ,**sub_rune[35],**sub_rune[36],**sub_rune[37],**sub_rune[38],**sub_rune[39],**sub_rune[40],**sub_rune[41]
               ,**sub_rune[42],**sub_rune[43],**sub_rune[44],**sub_rune[45],**sub_rune[46],**sub_rune[47],**sub_rune[48]
               ,**sub_rune[49],**sub_rune[50],**sub_rune[51],**sub_rune[52],**sub_rune[53],**sub_rune[54],**sub_rune[55]
               ,**sub_rune[56],**sub_rune[57],**sub_rune[58],**sub_rune[59],**sub_rune[60],**sub_rune[61],**sub_rune[62]}

# main 룬과 sub룬을 합침 -? rune_lst 룬id : 이미지 링크 매핑 -> 키값만 넣으면 바로 링크가 뽑힘.
rune_lst = {**main_rune, **sub_rune_lst}

# stat 룬 이미지는 해외 칼바람 사이트에서 사용하는 api를 가져옴
stat_rune = {"5008":'https://raw.communitydragon.org/11.24/plugins/rcp-be-lol-game-data/global/default/v1/perk-images/statmods/statmodsadaptiveforceicon.png',
"5002":'https://raw.communitydragon.org/11.24/plugins/rcp-be-lol-game-data/global/default/v1/perk-images/statmods/statmodsarmoricon.png',
"5005":'https://raw.communitydragon.org/11.24/plugins/rcp-be-lol-game-data/global/default/v1/perk-images/statmods/statmodsattackspeedicon.png',
"5007":'https://raw.communitydragon.org/11.24/plugins/rcp-be-lol-game-data/global/default/v1/perk-images/statmods/statmodscdrscalingicon.png',
"5001":'https://raw.communitydragon.org/11.24/plugins/rcp-be-lol-game-data/global/default/v1/perk-images/statmods/statmodshealthscalingicon.png',
"5003":'https://raw.communitydragon.org/11.24/plugins/rcp-be-lol-game-data/global/default/v1/perk-images/statmods/statmodsmagicresicon.png'}


champ_lst = list(df.groupby("CHAMPIONNAME").count().reset_index()["CHAMPIONNAME"])


# -

def test(df):
    # test는 임시변수명, 나중에 수정할 것
    test = df.groupby(["CHAMPIONNAME","MAIN_RUNE","SUB_RUNE","STAT_RUNE"])[["WIN"]].count().rename(columns={"WIN":"GAMES"})
    test.sort_values("GAMES", ascending=False, inplace=True)
    test.reset_index(inplace=True)

    test1 = df.groupby(["CHAMPIONNAME","MAIN_RUNE","SUB_RUNE","STAT_RUNE"])[["WIN"]].sum()
    test1.sort_values("WIN", ascending=False, inplace=True)
    test1.reset_index(inplace=True)

    test = pd.merge(test,test1, on=["CHAMPIONNAME","MAIN_RUNE","SUB_RUNE","STAT_RUNE"])
    test["rate"] = round(test.WIN / test.GAMES * 100)
    
    tmp = []
    for i in champ_lst:
        tmp_lst = []
        tmp_lst.append(test[test.CHAMPIONNAME == i].iloc[0]["CHAMPIONNAME"])
        tmp_lst.append(test[test.CHAMPIONNAME == i].iloc[0]["MAIN_RUNE"])
        tmp_lst.append(test[test.CHAMPIONNAME == i].iloc[0]["SUB_RUNE"])
        tmp_lst.append(test[test.CHAMPIONNAME == i].iloc[0]["STAT_RUNE"])
        tmp_lst.append(test[test.CHAMPIONNAME == i].iloc[0]["rate"])
        tmp.append(tmp_lst)


    PD = pd.DataFrame(tmp, columns=["CHAMPIONNAME","MAIN_RUNE","SUB_RUNE","STAT_RUNE","RATE"])

    PD["rune_img"] = PD.apply(lambda x: rune_img(x) ,axis=1)
    
    return PD

PD = test(df)

PD

champ_games = df.groupby("CHAMPIONNAME")[["GAMEID"]].count().rename(columns = {"GAMEID":"GAMES"}).reset_index()

champ_games

rune_games = df.groupby(["CHAMPIONNAME","MAIN_RUNE","SUB_RUNE","STAT_RUNE"])[["WIN"]].count().rename(columns={"WIN":"RUNE_GAMES"}).reset_index()
rune_win = df.groupby(["CHAMPIONNAME","MAIN_RUNE","SUB_RUNE","STAT_RUNE"])[["WIN"]].sum().reset_index()
rune_win_pick = pd.merge(pd.merge(rune_games, rune_win, on=["CHAMPIONNAME","MAIN_RUNE","SUB_RUNE","STAT_RUNE"]),champ_games, on="CHAMPIONNAME")
rune_win_pick["PICK_RATE"] = round(rune_win_pick.RUNE_GAMES / rune_win_pick.GAMES * 100,1)
rune_win_pick["WIN_RATE"] = round(rune_win_pick.WIN / rune_win_pick.RUNE_GAMES * 100,1)
rune_win_pick = rune_win_pick.sort_values("PICK_RATE", ascending=False)

rune_win_pick

rune_win_pick.groupby("CHAMPIONNAME")[["PICK_RATE"]].sum()

# #  과정

rune = df.iloc[0]["SUB_RUNE"].split("|")

rune

list(map(lambda x: 'https://ddragon.leagueoflegends.com/cdn/img/'+rune_lst[x], rune))

list(map(lambda x: 'https://ddragon.leagueoflegends.com/cdn/img/'+rune_lst[x], rune))

# 위에 있는 두개의 list(map())으로 메인룬, 서브룬의 이미지 url을 가져올 수 있음.

# - 적응형 - 5008
# - 공속 - 5005
# - 쿨감 - 5007
#
# - 체력 - 5001
# - 방어 - 5002
# - 마저 - 5003

stat_rune = {"5008":'https://raw.communitydragon.org/11.24/plugins/rcp-be-lol-game-data/global/default/v1/perk-images/statmods/statmodsadaptiveforceicon.png',
"5002":'https://raw.communitydragon.org/11.24/plugins/rcp-be-lol-game-data/global/default/v1/perk-images/statmods/statmodsarmoricon.png',
"5005":'https://raw.communitydragon.org/11.24/plugins/rcp-be-lol-game-data/global/default/v1/perk-images/statmods/statmodsattackspeedicon.png',
"5007":'https://raw.communitydragon.org/11.24/plugins/rcp-be-lol-game-data/global/default/v1/perk-images/statmods/statmodscdrscalingicon.png',
"5001":'https://raw.communitydragon.org/11.24/plugins/rcp-be-lol-game-data/global/default/v1/perk-images/statmods/statmodshealthscalingicon.png',
"5003":'https://raw.communitydragon.org/11.24/plugins/rcp-be-lol-game-data/global/default/v1/perk-images/statmods/statmodsmagicresicon.png'}

# stat_perk id : 이미지 링크 매핑
stat_rune

stat_perk = df.iloc[0]["STAT_RUNE"].split("|")

test = list(map(lambda x: stat_rune[x], stat_perk))

test

# ### --------------------------------------------------------------------------------------------------------------------------------------------------------

test = df.groupby(["CHAMPIONNAME","MAIN_RUNE","SUB_RUNE","STAT_RUNE"])[["WIN"]].count().rename(columns={"WIN":"GAMES"})
test.sort_values("GAMES", ascending=False, inplace=True)
test.reset_index(inplace=True)
test1 = df.groupby(["CHAMPIONNAME","MAIN_RUNE","SUB_RUNE","STAT_RUNE"])[["WIN"]].sum()
test1.sort_values("WIN", ascending=False, inplace=True)
test1.reset_index(inplace=True)
test = pd.merge(test,test1, on=["CHAMPIONNAME","MAIN_RUNE","SUB_RUNE","STAT_RUNE"])
test["rate"] = round(test.WIN / test.GAMES * 100)

test

url = 'http://ddragon.leagueoflegends.com/cdn/11.23.1/data/en_US/champion.json'
res = requests.get(url).json()
champ_lst = list(map(lambda x : (res['data'][x]['key'],x), list(res['data'].keys())))
champ_lst = dict(champ_lst)
champ_lst = list(champ_lst.values())
champ_lst[29] = "FiddleSticks"

# +
tmp = []
for i in champ_lst:
    tmp_lst = []
    tmp_lst.append(test[test.CHAMPIONNAME == i].iloc[0]["CHAMPIONNAME"])
    tmp_lst.append(test[test.CHAMPIONNAME == i].iloc[0]["MAIN_RUNE"])
    tmp_lst.append(test[test.CHAMPIONNAME == i].iloc[0]["SUB_RUNE"])
    tmp_lst.append(test[test.CHAMPIONNAME == i].iloc[0]["STAT_RUNE"])
    tmp_lst.append(test[test.CHAMPIONNAME == i].iloc[0]["rate"])
    tmp.append(tmp_lst)

#     베스트 룬 탑2를 할때 밑에를 추가 함. 딱 젤 많이 가는걸로 보여줄지는 생각해볼 필요가 있음
#     tmp_lst = []
#     tmp_lst.append(test[test.CHAMPIONNAME == i].iloc[1]["CHAMPIONNAME"])
#     tmp_lst.append(test[test.CHAMPIONNAME == i].iloc[1]["MAIN_RUNE"])
#     tmp_lst.append(test[test.CHAMPIONNAME == i].iloc[1]["SUB_RUNE"])
#     tmp_lst.append(test[test.CHAMPIONNAME == i].iloc[1]["STAT_RUNE"])
#     tmp_lst.append(test[test.CHAMPIONNAME == i].iloc[1]["rate"])
#     tmp.append(tmp_lst)
# -

PD = pd.DataFrame(tmp, columns=["CHAMPIONNAME","MAIN_RUNE","SUB_RUNE","STAT_RUNE","RATE"])


def rune_img(row):
    tmp = []
    tmp += list(map(lambda x: 'https://ddragon.leagueoflegends.com/cdn/img/'+rune_lst[x], row["MAIN_RUNE"].split("|")))
    tmp += list(map(lambda x: 'https://ddragon.leagueoflegends.com/cdn/img/'+rune_lst[x], row["SUB_RUNE"].split("|")))
    tmp += list(map(lambda x: stat_rune[x], row["STAT_RUNE"].split("|")))
    # tmp += str(int(row["RATE"])) 넣으면 숫자가 한자리씩 |가 들어감.. 일단 보류
    tmp = "|".join(tmp)
    return tmp


PD["rune_img"] = PD.apply(lambda x: rune_img(x) ,axis=1)

PD

# # 2.

# +
# 먼저 메인룬만 뽑아옴
url = "https://ddragon.leagueoflegends.com/cdn/11.24.1/data/ko_KR/runesReforged.json"
res = requests.get(url).json()
main_rune = list(map(lambda x: ({str(x["id"]):x["icon"]}), res))
main_rune = {**main_rune[0],**main_rune[1],**main_rune[2],**main_rune[3],**main_rune[4]}

# 서브룬을 만들어 줌
sub_rune = list(map(lambda x: list(map(lambda z: list(map(lambda c: ({str(c["id"]):c["icon"]}), z["runes"])), x["slots"])), res))

# 3차원배열이여서 1차원으로 축소
sub_rune = [element for array in sub_rune for element in array]
sub_rune = [element for array in sub_rune for element in array]

# key값으로 검색할 수 있게 하나의 딕셔너리로 만드는 과정
sub_rune_lst = {**sub_rune[0],**sub_rune[1],**sub_rune[2],**sub_rune[3],**sub_rune[4],**sub_rune[5],**sub_rune[6]
               ,**sub_rune[7],**sub_rune[8],**sub_rune[9],**sub_rune[10],**sub_rune[11],**sub_rune[12],**sub_rune[13]
               ,**sub_rune[14],**sub_rune[15],**sub_rune[16],**sub_rune[17],**sub_rune[18],**sub_rune[19],**sub_rune[20]
               ,**sub_rune[21],**sub_rune[22],**sub_rune[23],**sub_rune[24],**sub_rune[25],**sub_rune[26],**sub_rune[27]
               ,**sub_rune[28],**sub_rune[29],**sub_rune[30],**sub_rune[31],**sub_rune[32],**sub_rune[33],**sub_rune[34]
               ,**sub_rune[35],**sub_rune[36],**sub_rune[37],**sub_rune[38],**sub_rune[39],**sub_rune[40],**sub_rune[41]
               ,**sub_rune[42],**sub_rune[43],**sub_rune[44],**sub_rune[45],**sub_rune[46],**sub_rune[47],**sub_rune[48]
               ,**sub_rune[49],**sub_rune[50],**sub_rune[51],**sub_rune[52],**sub_rune[53],**sub_rune[54],**sub_rune[55]
               ,**sub_rune[56],**sub_rune[57],**sub_rune[58],**sub_rune[59],**sub_rune[60],**sub_rune[61],**sub_rune[62]}

# main 룬과 sub룬을 합침 -? rune_lst 룬id : 이미지 링크 매핑 -> 키값만 넣으면 바로 링크가 뽑힘.
rune_lst = {**main_rune, **sub_rune_lst}

# stat 룬 이미지는 해외 칼바람 사이트에서 사용하는 api를 가져옴
stat_rune = {"5008":'https://raw.communitydragon.org/11.24/plugins/rcp-be-lol-game-data/global/default/v1/perk-images/statmods/statmodsadaptiveforceicon.png',
"5002":'https://raw.communitydragon.org/11.24/plugins/rcp-be-lol-game-data/global/default/v1/perk-images/statmods/statmodsarmoricon.png',
"5005":'https://raw.communitydragon.org/11.24/plugins/rcp-be-lol-game-data/global/default/v1/perk-images/statmods/statmodsattackspeedicon.png',
"5007":'https://raw.communitydragon.org/11.24/plugins/rcp-be-lol-game-data/global/default/v1/perk-images/statmods/statmodscdrscalingicon.png',
"5001":'https://raw.communitydragon.org/11.24/plugins/rcp-be-lol-game-data/global/default/v1/perk-images/statmods/statmodshealthscalingicon.png',
"5003":'https://raw.communitydragon.org/11.24/plugins/rcp-be-lol-game-data/global/default/v1/perk-images/statmods/statmodsmagicresicon.png'}

champ_lst = list(df.groupby("CHAMPIONNAME").count().reset_index()["CHAMPIONNAME"])


# -

def champ_rune_build(df):
    
    df["MAIN"] = df.apply(lambda x: x["MAIN_RUNE"].split("|")[0], axis=1)
    df["SUB"] = df.apply(lambda x: x["SUB_RUNE"].split("|")[0], axis=1)
    
    rune_games = df.groupby(["CHAMPIONNAME","MAIN_RUNE","SUB_RUNE","STAT_RUNE"])[["WIN"]].count().rename(columns={"WIN":"RUNE_GAMES"}).reset_index()
    rune_win = df.groupby(["CHAMPIONNAME","MAIN_RUNE","SUB_RUNE","STAT_RUNE"])[["WIN"]].sum().reset_index()
    rune_win_pick = pd.merge(pd.merge(rune_games, rune_win, on=["CHAMPIONNAME","MAIN_RUNE","SUB_RUNE","STAT_RUNE"]),champ_games, on="CHAMPIONNAME")
    rune_win_pick["PICK_RATE"] = round(rune_win_pick.RUNE_GAMES / rune_win_pick.GAMES * 100,1)
    rune_win_pick["WIN_RATE"] = round(rune_win_pick.WIN / rune_win_pick.RUNE_GAMES * 100,1)
    rune_win_pick = rune_win_pick.sort_values("PICK_RATE", ascending=False)
    rune_win_pick["MAIN"] = rune_win_pick.apply(lambda x: x["MAIN_RUNE"].split("|")[0], axis=1)
    rune_win_pick["SUB"] = rune_win_pick.apply(lambda x: x["SUB_RUNE"].split("|")[0], axis=1)
    rune_build = rune_win_pick


    rune_games = df.groupby(["CHAMPIONNAME","MAIN","SUB"])[["WIN"]].count().rename(columns={"WIN":"RUNE_GAMES"}).reset_index()
    rune_win = df.groupby(["CHAMPIONNAME","MAIN","SUB"])[["WIN"]].sum().reset_index()
    rune_win_pick = pd.merge(pd.merge(rune_games, rune_win, on=["CHAMPIONNAME","MAIN","SUB"]),champ_games, on="CHAMPIONNAME")
    rune_win_pick["PICK_RATE"] = round(rune_win_pick.RUNE_GAMES / rune_win_pick.GAMES * 100,1)
    rune_win_pick["WIN_RATE"] = round(rune_win_pick.WIN / rune_win_pick.RUNE_GAMES * 100,1)
    rune_win_pick = rune_win_pick.sort_values("PICK_RATE", ascending=False)
    main_sub_rune = rune_win_pick[["CHAMPIONNAME","MAIN","SUB","PICK_RATE","WIN_RATE"]].rename(columns={"PICK_RATE":"MAIN_PICK_RATE","WIN_RATE":"MAIN_WIN_RATE"})

    rune = pd.merge(rune_build,main_sub_rune, on=["CHAMPIONNAME","MAIN","SUB"]).sort_values(["MAIN_PICK_RATE","PICK_RATE"], ascending=False)
    
    tmp = []
    for i in champ_lst:
        tmp_lst = []
        champ = rune[rune.CHAMPIONNAME == i]
        tmp_lst.append(champ.iloc[0]["CHAMPIONNAME"])
        tmp_lst.append(champ.iloc[0]["MAIN_RUNE"])
        tmp_lst.append(champ.iloc[0]["SUB_RUNE"])
        tmp_lst.append(champ.iloc[0]["STAT_RUNE"])
        tmp_lst.append(champ.iloc[0]["PICK_RATE"])
        tmp_lst.append(champ.iloc[0]["WIN_RATE"])
        tmp_lst.append(champ.iloc[0]["MAIN"])
        tmp_lst.append(champ.iloc[0]["SUB"])
        tmp_lst.append(champ.iloc[0]["MAIN_PICK_RATE"])
        tmp_lst.append(champ.iloc[0]["MAIN_WIN_RATE"])

        s_champ = champ[champ.MAIN_PICK_RATE < champ.iloc[0]["MAIN_PICK_RATE"]]
        tmp_lst.append(s_champ.iloc[0]["MAIN_RUNE"])
        tmp_lst.append(s_champ.iloc[0]["SUB_RUNE"])
        tmp_lst.append(s_champ.iloc[0]["STAT_RUNE"])
        tmp_lst.append(s_champ.iloc[0]["PICK_RATE"])
        tmp_lst.append(s_champ.iloc[0]["WIN_RATE"])
        tmp_lst.append(s_champ.iloc[0]["MAIN"])
        tmp_lst.append(s_champ.iloc[0]["SUB"])
        tmp_lst.append(s_champ.iloc[0]["MAIN_PICK_RATE"])
        tmp_lst.append(s_champ.iloc[0]["MAIN_WIN_RATE"])

        tmp.append(tmp_lst)

    champ_rune = pd.DataFrame(tmp,columns=["CHAMPIONNAME","F_MAIN_RUNE","F_SUB_RUNE","F_STAT_RUNE","F_PICK_RATE","F_WIN_RATE","F_MAIN","F_SUB",
                                           "F_MAIN_PICK_RATE","F_MAIN_WIN_RATE",
                                           "S_MAIN_RUNE","S_SUB_RUNE","S_STAT_RUNE","S_PICK_RATE","S_WIN_RATE","S_MAIN","S_SUB",
                                           "S_MAIN_PICK_RATE","S_MAIN_WIN_RATE"])

    champ_rune["RUNE_IMG_ONE"] = champ_rune.apply(lambda x: rune_img_one(x), axis=1)
    champ_rune["RUNE_IMG_TWO"] = champ_rune.apply(lambda x: rune_img_two(x), axis=1)

    return champ_rune


def rune_img_one(row):
    tmp = []
    tmp += list(map(lambda x: 'https://ddragon.leagueoflegends.com/cdn/img/'+rune_lst[x], row["F_MAIN_RUNE"].split("|")))
    tmp += list(map(lambda x: 'https://ddragon.leagueoflegends.com/cdn/img/'+rune_lst[x], row["F_SUB_RUNE"].split("|")))
    tmp += list(map(lambda x: stat_rune[x], row["F_STAT_RUNE"].split("|")))
    tmp += [str(row.F_MAIN_PICK_RATE)]
    tmp += [str(row.F_MAIN_WIN_RATE)]
    
    tmp = "|".join(tmp)
    return tmp


def rune_img_two(row):
    tmp = []    
    tmp += list(map(lambda x: 'https://ddragon.leagueoflegends.com/cdn/img/'+rune_lst[x], row["S_MAIN_RUNE"].split("|")))
    tmp += list(map(lambda x: 'https://ddragon.leagueoflegends.com/cdn/img/'+rune_lst[x], row["S_SUB_RUNE"].split("|")))
    tmp += list(map(lambda x: stat_rune[x], row["S_STAT_RUNE"].split("|")))
    tmp += [str(row.S_MAIN_PICK_RATE)]
    tmp += [str(row.S_MAIN_WIN_RATE)]
    
    tmp = "|".join(tmp)
    return tmp


champ_rune = champ_rune_build(df)

champ_rune[["CHAMPIONNAME","RUNE_IMG_ONE","RUNE_IMG_TWO"]]
