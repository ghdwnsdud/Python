import cx_Oracle
import pandas as pd
import requests
from numpy import inf
from tqdm import tqdm
tqdm.pandas()
from random import sample
import random


dsn = cx_Oracle.makedsn('',,'xe') # ip와 

# db 연결
def db_open():
    global db
    global cursor
    try:
        db = cx_Oracle.connect('','',dsn) # id와 pw
        cursor = db.cursor()
        return print("OPEN")
    except Exception as e:
        print(e)
        
# db 연결해제    
def db_close():
    global db
    global cursor
    try:
        db.commit()
        cursor.close()
        db.close()
        return print("close")
    except Exception as e:
        print(e)
        
# df에 전송하는 sql문 (sql문을 써서 넘김)        
def sql(query):
    global db
    global cursor
    try:
        if (query.split()[0] == 'select'):
            df = pd.read_sql(sql=query, con=db)
            return df
        cursor.execute(query)
        return 'success!'
    except Exception as e:
        print(e)
        
# 나누어져 있는 sepll을 리스트화 해주는 작업
def sepll_lst(row):
    if row["SPELL1"] == 4:
        tmp = [row["SPELL2"],row["SPELL1"]]
    elif row["SPELL2"] == 4:
        tmp = [row["SPELL1"],row["SPELL2"]]
    elif row["SPELL1"] == 32:
        tmp = [row["SPELL1"],row["SPELL2"]]
    elif row["SPELL2"] == 32:
        tmp = [row["SPELL2"],row["SPELL2"]]
    else:
        tmp = [row["SPELL2"],row["SPELL1"]]
        tmp.sort()
    tmp = list(map(str,tmp))
    tmp = "|".join(tmp)
    return tmp

# 가장 많이 선택한 rune 첫번째를 url+픽률과 승률 한줄로 정리
def rune_img_one(row):
    tmp = []
    tmp += list(map(lambda x: 'https://ddragon.leagueoflegends.com/cdn/img/'+rune_lst[x], row["F_MAIN_RUNE"].split("|")))
    tmp += list(map(lambda x: 'https://ddragon.leagueoflegends.com/cdn/img/'+rune_lst[x], row["F_SUB_RUNE"].split("|")))
    tmp += list(map(lambda x: stat_rune[x], row["F_STAT_RUNE"].split("|")))
    tmp += [str(row.F_MAIN_PICK_RATE)+"%"]
    tmp += [str(row.F_MAIN_WIN_RATE)+"%"]
    
    tmp = "|".join(tmp)
    return tmp

# 가장 많이 선택한 rune 두번째를 url+픽률과 승률 한줄로 정리
def rune_img_two(row):
    tmp = []    
    tmp += list(map(lambda x: 'https://ddragon.leagueoflegends.com/cdn/img/'+rune_lst[x], row["S_MAIN_RUNE"].split("|")))
    tmp += list(map(lambda x: 'https://ddragon.leagueoflegends.com/cdn/img/'+rune_lst[x], row["S_SUB_RUNE"].split("|")))
    tmp += list(map(lambda x: stat_rune[x], row["S_STAT_RUNE"].split("|")))
    tmp += [str(row.S_MAIN_PICK_RATE)+"%"]
    tmp += [str(row.S_MAIN_WIN_RATE)+"%"]
    
    tmp = "|".join(tmp)
    return tmp

# return champ_rune은 챔피언의 베스트 rune top 1,2의 픽률과 승률이 담겨져 있다.
def champ_rune_build(df):
    
    df["MAIN"] = df.apply(lambda x: x["MAIN_RUNE"].split("|")[0], axis=1)
    df["SUB"] = df.apply(lambda x: x["SUB_RUNE"].split("|")[0], axis=1)
    
    champ_games = df.groupby("CHAMPIONNAME")[["GAMEID"]].count().rename(columns = {"GAMEID":"GAMES"}).reset_index()
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



# 가장 많이 선택한 sepll 2가지를 url+픽률과 승률 한줄로 정리
def spell_img(row):
    tmp = []
    tmp += list(map(lambda x: spell[x], row["F_SEPLL"].split("|")))
    tmp += [str(row.F_PICK_RATE)+"%"]
    tmp += [str(row.F_WIN_RATE)+"%"]
    
    tmp += list(map(lambda x: spell[x], row["S_SEPLL"].split("|")))
    tmp += [str(row.S_PICK_RATE)+"%"]
    tmp += [str(row.S_WIN_RATE)+"%"]
    
    tmp = "|".join(tmp)
    return tmp

# return champ_spell은 챔피언의 베스트 spell top 1,2의 픽률과 승률이 담겨져 있다.
def champ_spell_win_pick_rate(df):
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

    return champ_spell




# 가장 많이 선택한 starting_item 첫번째를 url+픽률과 승률 한줄로 정리
def starting_item_img_one(row):
    tmp = []
    tmp += list(map(lambda x: item_lst[x], row["F_STARTING_ITEM"].split("|")))
    tmp += [str(row.F_PICK_RATE)+"%"]
    tmp += [str(row.F_WIN_RATE)+"%"]
    
    tmp = "|".join(tmp)
    return tmp
# 가장 많이 선택한 starting_item 두번째를 url+픽률과 승률 한줄로 정리
def starting_item_img_two(row):
    tmp = []
    tmp += list(map(lambda x: item_lst[x], row["S_STARTING_ITEM"].split("|")))
    tmp += [str(row.S_PICK_RATE)+"%"]
    tmp += [str(row.S_WIN_RATE)+"%"]
    
    tmp = "|".join(tmp)
    return tmp

# return champ_starting_item은 챔피언의 베스트 starting_item top 1,2의 픽률과 승률이 담겨져 있다.
def starting_item(df):
    champ_games = df.groupby("CHAMPIONNAME")[["GAMEID"]].count().rename(columns = {"GAMEID":"GAMES"}).reset_index()
    starting_item_games = df.groupby(["CHAMPIONNAME","STARTING_ITEM"])[["WIN"]].count().rename(columns={"WIN":"STARTING_ITEMS_GAMES"}).reset_index()
    starting_item_win = df.groupby(["CHAMPIONNAME","STARTING_ITEM"])[["WIN"]].sum().reset_index()
    starting_item_win_pick = pd.merge(pd.merge(starting_item_games, starting_item_win, on=["CHAMPIONNAME","STARTING_ITEM"]),champ_games,
                                      on="CHAMPIONNAME")
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
        
    champ_starting_item = pd.DataFrame(tmp,columns=
                                       ["CHAMPIONNAME","F_STARTING_ITEM","F_PICK_RATE","F_WIN_RATE","S_STARTING_ITEM","S_PICK_RATE","S_WIN_RATE"])

    champ_starting_item["STARTING_ITEM_IMG_ONE"] = champ_starting_item.apply(lambda x: starting_item_img_one(x), axis=1)
    
    champ_starting_item["STARTING_ITEM_IMG_TWO"] = champ_starting_item.apply(lambda x: starting_item_img_two(x), axis=1)

    return champ_starting_item


# 가장 많이 선택한 main_item 3종류를 url+픽률과 승률 한줄로 정리
def main_item_img(row):
    tmp = []
    tmp += list(map(lambda x: item_lst[x], row["F_MAIN_ITEM_BUILD"].split("|")))
    tmp += [str(row.F_PICK_RATE)+"%"]
    tmp += [str(row.F_WIN_RATE)+"%"]
    
    tmp += list(map(lambda x: item_lst[x], row["S_MAIN_ITEM_BUILD"].split("|")))
    tmp += [str(row.S_PICK_RATE)+"%"]
    tmp += [str(row.S_WIN_RATE)+"%"]
    
    tmp += list(map(lambda x: item_lst[x], row["T_MAIN_ITEM_BUILD"].split("|")))
    tmp += [str(row.T_PICK_RATE)+"%"]
    tmp += [str(row.T_WIN_RATE)+"%"]
    
    tmp = "|".join(tmp)
    return tmp

# return champ_main_item_build는 챔피언의 베스트 main_item top 1,2,3의 픽률과 승률이 담겨져 있다.
def main_item(df):
    mi_df = df[df['MAIN_ITEM_BUILD'].notna()]
    boolean = mi_df["MAIN_ITEM_BUILD"].apply(lambda x: x.count("|") == 2)
    mi_df = mi_df[boolean]
    champ_games = mi_df.groupby("CHAMPIONNAME")[["GAMEID"]].count().rename(columns = {"GAMEID":"GAMES"}).reset_index()
    main_item_games = mi_df.groupby(["CHAMPIONNAME","MAIN_ITEM_BUILD"])[["WIN"]].count().rename(columns={"WIN":"MAIN_ITEMS_GAMES"}).reset_index()
    main_item_win = mi_df.groupby(["CHAMPIONNAME","MAIN_ITEM_BUILD"])[["WIN"]].sum().reset_index()
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

    champ_main_item_build = pd.DataFrame(tmp,columns=
                                        ["CHAMPIONNAME","F_MAIN_ITEM_BUILD","F_PICK_RATE","F_WIN_RATE","S_MAIN_ITEM_BUILD","S_PICK_RATE","S_WIN_RATE"
                                                     ,"T_MAIN_ITEM_BUILD","T_PICK_RATE","T_WIN_RATE"])
    champ_main_item_build["MAIN_ITEM_BUILD_IMG"] = champ_main_item_build.apply(lambda x: main_item_img(x), axis=1)
    
    return champ_main_item_build



# 가장 많이 선택한 boots 2종류를 url+픽률과 승률 한줄로 정리
def boots_img(row):
    if row.CHAMPIONNAME != "Cassiopeia":
        tmp = []
        try:
            tmp += list(map(lambda x: item_lst[x], str(row["F_BOOTS"]).split("|")))
        except:
            tmp += ["NoShoes"]
            
        tmp += [str(row.F_PICK_RATE)+"%"]
        tmp += [str(row.F_WIN_RATE)+"%"]
        
        try:
            tmp += list(map(lambda x: item_lst[x], str(row["S_BOOTS"]).split("|")))
        except:
            tmp += ["NoShoes"]
            
        tmp += [str(row.S_PICK_RATE)+"%"]
        tmp += [str(row.S_WIN_RATE)+"%"]
       
        tmp = "|".join(tmp)
    else:
        tmp = "0"
    return tmp

# return champ_boots는 챔피언의 베스트 boots top 1,2의 픽률과 승률이 담겨져 있다.
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

# 챔피언의 스킬 (qwe) url 매칭
def champ_skill(x):
    url = "https://ddragon.leagueoflegends.com/cdn/11.24.1/data/ko_KR/champion/"+x+".json"
    res = requests.get(url).json()
    Q = "http://ddragon.leagueoflegends.com/cdn/11.24.1/img/spell/"+res["data"][x]["spells"][0]["id"]+".png"
    W = "http://ddragon.leagueoflegends.com/cdn/11.24.1/img/spell/"+res["data"][x]["spells"][1]["id"]+".png"
    E = "http://ddragon.leagueoflegends.com/cdn/11.24.1/img/spell/"+res["data"][x]["spells"][2]["id"]+".png"
    
    champ_qwe = {"1":Q, "2":W, "3":E}
    
    return champ_qwe

# 챔피언의 스킬트리 정렬 ( 가장 먼저 마스터한 순서대로 )
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

# 스킬마스터 순으로 QWE
def skill_k(row):
    skill_key = {"1":"Q","2":"W","3":"E"} 
    tmp = []
    tmp = list(map(lambda x: skill_key[x], row.SKILL_BUILD.split("|")))
    tmp = "|".join(tmp)
    return tmp


# 가장 많이 선택한 스킬을 url+픽률과 승률 한줄로 정리
def skill_img(row):
    tmp = []
    tmp += list(map(lambda x: champ_skill_img[row.CHAMPIONNAME][x], row["SKILL_BUILD"].split("|")))
    tmp += [str(row.PICK_RATE)+"%"]
    tmp += [str(row.WIN_RATE)+"%"]
    
    tmp = "|".join(tmp)
    
    return tmp

# return champ_skill_build는 챔피언의 베스트 스킬 top 1의 픽률과 승률이 담겨져 있다.
def skill(df):
    champ_games = df.groupby("CHAMPIONNAME")[["GAMEID"]].count().rename(columns = {"GAMEID":"GAMES"}).reset_index()
    df["SKILL_BUILD"] = df.apply(lambda x: x.SKILL_BUILD.split("|"), axis=1)
    df["SKILL_BUILD"] = df.apply(lambda x: skill_tree(x), axis=1)
    skill_games = df.groupby(["CHAMPIONNAME","SKILL_BUILD"])[["WIN"]].count().rename(columns={"WIN":"SKILL_GAMES"}).reset_index()
    skill_win = df.groupby(["CHAMPIONNAME","SKILL_BUILD"])[["WIN"]].sum().reset_index()
    skill_win_rate = pd.merge(pd.merge(skill_games, skill_win, on=["CHAMPIONNAME","SKILL_BUILD"]),champ_games,on="CHAMPIONNAME")
    skill_win_rate["PICK_RATE"] = round(skill_win_rate.SKILL_GAMES/skill_win_rate.GAMES *100 , 1)
    skill_win_rate["WIN_RATE"] = round(skill_win_rate.WIN/skill_win_rate.SKILL_GAMES *100 , 1)
    skill_win_rate = skill_win_rate.sort_values("PICK_RATE", ascending=False)

    tmp = []
    for i in champ_lst:
        tmp_lst = []
        tmp_lst.append(skill_win_rate[skill_win_rate.CHAMPIONNAME == i].iloc[0]["CHAMPIONNAME"])
        tmp_lst.append(skill_win_rate[skill_win_rate.CHAMPIONNAME == i].iloc[0]["SKILL_BUILD"])
        tmp_lst.append(skill_win_rate[skill_win_rate.CHAMPIONNAME == i].iloc[0]["PICK_RATE"])
        tmp_lst.append(skill_win_rate[skill_win_rate.CHAMPIONNAME == i].iloc[0]["WIN_RATE"])

        tmp.append(tmp_lst)

    champ_skill_build = pd.DataFrame(tmp,columns=["CHAMPIONNAME","SKILL_BUILD","PICK_RATE","WIN_RATE"])

    champ_skill_build["SKILL_IMG"] = champ_skill_build.apply(lambda x: skill_img(x), axis=1)
    champ_skill_build["SKILL_KEY"] = champ_skill_build.apply(lambda x: skill_k(x), axis=1)

    return champ_skill_build

# combination_score로 조합승률 분류
def rank(row):
    if 20 > row.COMBINATION_SCORE >= 5:
        return "A"
    elif 5 > row.COMBINATION_SCORE > -5:
        return "C"
    elif -30 < row.COMBINATION_SCORE < -5:
        return "B"

# combination_score type을 문자열로 바꿔주는 함수
def score_str(row):
    a = "{:.1f}%".format(row.COMBINATION_SCORE)
    return a

# 팀 조합승률 구하는 함수
def combination(df):
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
    synergy = pd.merge(synergy1, combination_rate_ko_name ,on="CHAMPIONNAME").rename(columns={"CHAMPNAME":"K_SYNERGY"})

    return synergy





# 최종적으로 DB에 들어가게될 DF
# 인자값 df는 원시데이터 DF가 들어가게 된다.
def final_df(df):
    
    global champ_lst
    global rune_lst
    global stat_rune
    global spell
    global item_lst
    global champ_skill_img
    
    
    df = df[df['SKILL_BUILD'].notna()]
    # 챔피언 리스트 담아두기
    champ_lst = list(df.groupby("CHAMPIONNAME").count().reset_index()["CHAMPIONNAME"])
    
    # 룬 정보를 가져옴
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

    # main 룬과 sub룬을 합침 -> rune_lst 룬id : 이미지 링크 매핑 -> 키값만 넣으면 바로 링크가 뽑힘.
    rune_lst = {**main_rune, **sub_rune_lst}

    # stat 룬 이미지는 해외 칼바람 사이트에서 사용하는 api를 가져옴
    stat_rune = {
    "5008":'https://raw.communitydragon.org/11.24/plugins/rcp-be-lol-game-data/global/default/v1/perk-images/statmods/statmodsadaptiveforceicon.png',
    "5002":'https://raw.communitydragon.org/11.24/plugins/rcp-be-lol-game-data/global/default/v1/perk-images/statmods/statmodsarmoricon.png',
    "5005":'https://raw.communitydragon.org/11.24/plugins/rcp-be-lol-game-data/global/default/v1/perk-images/statmods/statmodsattackspeedicon.png',
    "5007":'https://raw.communitydragon.org/11.24/plugins/rcp-be-lol-game-data/global/default/v1/perk-images/statmods/statmodscdrscalingicon.png',
    "5001":'https://raw.communitydragon.org/11.24/plugins/rcp-be-lol-game-data/global/default/v1/perk-images/statmods/statmodshealthscalingicon.png',
    "5003":'https://raw.communitydragon.org/11.24/plugins/rcp-be-lol-game-data/global/default/v1/perk-images/statmods/statmodsmagicresicon.png'}
    
    
    # spell 의 key와 이미지 url 매칭
    url = "https://ddragon.leagueoflegends.com/cdn/11.24.1/data/ko_KR/summoner.json"
    res = requests.get(url).json()
    spellId = list(res["data"].keys())
    spellkey = list(map(lambda x: res["data"][x]["key"], spellId))
    spellurl = list(map(lambda x: "https://ddragon.leagueoflegends.com/cdn/11.24.1/img/spell/"+x+".png", spellId))
    spell = dict(zip(spellkey,spellurl)) # spell_img 함수에서 사용
    df["SEPLL_LST"] = df.apply(lambda x: sepll_lst(x), axis=1)
    
    # 아이템의 key와 이미지 url 매칭
    url = "http://ddragon.leagueoflegends.com/cdn/11.24.1/data/ko_KR/item.json"
    res = requests.get(url).json()
    item_key = list(res['data'].keys())
    itemurl = list(map(lambda x: "http://ddragon.leagueoflegends.com/cdn/11.24.1/img/item/"+x+".png", item_key))
    item_lst = dict(zip(item_key,itemurl)) # starting_item_img, main_item_img 함수에서 사용
    
    # 챔피언의 이름과 스킬(qwe) url 매칭
    url ="https://ddragon.leagueoflegends.com/cdn/11.24.1/data/ko_KR/champion.json"
    res = requests.get(url).json()
    champion = list(res["data"].keys())
    champ_json = list(map(lambda x: "https://ddragon.leagueoflegends.com/cdn/11.24.1/data/ko_KR/champion/"+x+".json", champion))
    champ_skill_img = list(map(lambda x: champ_skill(x), champion))
    num = champion.index("Fiddlesticks")
    champion[num] = "FiddleSticks"
    champ_skill_img = dict(zip(champion,champ_skill_img))
    
    
    
    
    # df의 WIN컬럼 0이나 1로 바꿔주기.
    df["WIN"] = df.apply(lambda x: 1 if x["WIN"] == True else 0 , axis=1)

    # 챔피언의 rune
    champ_rune = champ_rune_build(df)
    
    # 챔피언의 spell
    champ_spell = champ_spell_win_pick_rate(df)
    
    # 챔피언의 starting_item
    champ_starting_item = starting_item(df)
    
    # 챔피언의 main_item
    champ_main_item_build = main_item(df)
    
    # 챔피언의 boots
    champ_boots = boots_build(df)
    
    # 챔피언의 skill_build
    champ_skill_build = skill(df)
    
    # 챔피언의 team_synergy
    synergy = combination(df)
    
    final_df = pd.concat([champ_rune,champ_spell,champ_starting_item,champ_main_item_build,champ_boots,champ_skill_build,synergy], axis=1,
                         join="inner")
    
    return final_df
