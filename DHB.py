import cx_Oracle
import pandas as pd
import requests
from numpy import inf
from tqdm import tqdm
tqdm.pandas()
from random import sample
import random


dsn = cx_Oracle.makedsn('121.65.47.77',6000,'xe') # 수정예정

# RGAPI-86d239db-bfc9-4f08-acd4-f110b451241a 메인
# RGAPI-802e53c9-4a10-43de-8a1d-4b33199a3b2c 서브
riot_api_key = 'RGAPI-86d239db-bfc9-4f08-acd4-f110b451241a' # api는 자기 라이엇 api사용

# db 연결
def db_open():
    global db
    global cursor
    try:
        db = cx_Oracle.connect('DHB','1111',dsn) # id와 pw 알려준걸로 기입
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
# if문에서 'select' in query 로 바꿀수도 있다.

# puuid_lst 얻어오기
def puuid_lst(name):
    url = "https://kr.api.riotgames.com/lol/summoner/v4/summoners/by-name/"+name+"?api_key="+riot_api_key
    res = requests.get(url).json()
    return res["puuid"]

# match_id 얻어오기
def get_match_id(puuid,num):
    url ='https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/'+puuid+'/ids?api_key='+riot_api_key+'&start=0&count='+str(num)
    res = requests.get(url).json()
    return res

# match, timeline 얻어오기
def get_riot_match(matchid):
    url = 'https://asia.api.riotgames.com/lol/match/v5/matches/'+matchid+'?api_key='+riot_api_key
    res1 = requests.get(url).json()
    url = 'https://asia.api.riotgames.com/lol/match/v5/matches/'+matchid+'/timeline?api_key='+riot_api_key
    res2 = requests.get(url).json()
    return res1,res2

def get_aram():
    url = 'https://kr.api.riotgames.com/lol/spectator/v4/featured-games?api_key='+riot_api_key
    res = requests.get(url).json()
    aram_lst = []
    
    # 현재 플레이중인 칼바람 유저들의 summonerName을 가져온다.
    try:
        aram_lst = filter(lambda x: x["gameMode"] == "ARAM",res["gameList"])
    except:
        pass
    if type(aram_lst) == 'filter':
        return aram_lst
    
    aram_lst = list(filter(lambda x: x["gameMode"] == "ARAM",res["gameList"]))
    aram_p_lst = list(map(lambda x: x["participants"][0]["summonerName"], aram_lst)) # participants 뒤의 수는 0~9 까지 임의로 지정 가능
                                                                                     # 팀원들은 수가 겹치지 않게 전부 다르게 할것.
    
    id_lst = list(map(lambda x: puuid_lst(x), aram_p_lst))
    
    try:
        num = int(40/len(aram_p_lst))
    except:
        num = 0
    
    # match_id를 담는다.
    match_id = []
    for i in id_lst:
        match_id += get_match_id(i,num)

    # DataFrame화 시키기.
    tmp = []
    for i in tqdm(match_id):
        tmp_lst = []
        tmp_lst.append(i)
        matches, timelines = get_riot_match(i)
        tmp_lst.append(matches)
        tmp_lst.append(timelines)
        tmp.append(tmp_lst)
    df = pd.DataFrame(tmp, columns=['gameid','matches','timelines'])
    print("complete...!")
    return df

# 컬럼으로 만들기
def preprocessing(df):
    match_df_lst = []

    #아이템 리스트 만들기
    url = "http://ddragon.leagueoflegends.com/cdn/11.24.1/data/ko_KR/item.json"
    res = requests.get(url).json()
    item_key = list(res['data'].keys()) # item_key는 모든 item의 id값이 들어있음
    l_item = list(filter(lambda x: res['data'][x]["gold"]["total"] > 2299, item_key)) # 아이템의 골드가격 기준으로 전설/신화 구분
    boots = ['3006','3009','3020','3047','3158','3117','3111']
    # match_df
    print("get matches & timelines...")
    for i in range(len(df)):
        try:
            if df.iloc[i].matches['info']['gameMode'] == 'ARAM':
                for j in range(10):
                    tmp_lst = []
                    tmp_lst.append(df.iloc[i].gameid)
                    tmp_lst.append(df.iloc[i].matches['info']['participants'][j]['participantId'])
                    participantId = df.iloc[i].matches['info']['participants'][j]['participantId']

                    tmp_lst.append(df.iloc[i].matches['info']['participants'][j]['championId'])
                    tmp_lst.append(df.iloc[i].matches['info']['participants'][j]['championName'])
                    tmp_lst.append(df.iloc[i].matches['info']['participants'][j]['summoner1Id'])
                    tmp_lst.append(df.iloc[i].matches['info']['participants'][j]['summoner2Id'])

                    main_rune_build = []
                    main_rune_build.append(str(df.iloc[i].matches['info']['participants'][j]['perks']['styles'][0]['style']))
                    main_rune_build.append(str(df.iloc[i].matches['info']['participants'][j]['perks']['styles'][0]['selections'][0]['perk']))
                    main_rune_build.append(str(df.iloc[i].matches['info']['participants'][j]['perks']['styles'][0]['selections'][1]['perk']))
                    main_rune_build.append(str(df.iloc[i].matches['info']['participants'][j]['perks']['styles'][0]['selections'][2]['perk']))
                    main_rune_build.append(str(df.iloc[i].matches['info']['participants'][j]['perks']['styles'][0]['selections'][3]['perk']))
                    tmp_lst.append("|".join(main_rune_build))
                    
                    sub_rune_build = []
                    sub_rune_build.append(str(df.iloc[i].matches['info']['participants'][j]['perks']['styles'][1]['style']))
                    sub_rune_build.append(str(df.iloc[i].matches['info']['participants'][j]['perks']['styles'][1]['selections'][0]['perk']))
                    sub_rune_build.append(str(df.iloc[i].matches['info']['participants'][j]['perks']['styles'][1]['selections'][1]['perk']))
                    tmp_lst.append("|".join(sub_rune_build))
                    
                    stat_rune_build = []
                    stat_rune_build.append(str(df.iloc[i].matches['info']['participants'][j]['perks']['statPerks']['defense']))
                    stat_rune_build.append(str(df.iloc[i].matches['info']['participants'][j]['perks']['statPerks']['flex']))
                    stat_rune_build.append(str(df.iloc[i].matches['info']['participants'][j]['perks']['statPerks']['offense']))
                    tmp_lst.append("|".join(stat_rune_build))

                    tmp_lst.append(df.iloc[i].matches['info']['participants'][j]['teamId'])
                    tmp_lst.append(df.iloc[i].matches['info']['participants'][j]['win'])
                    tmp_lst.append(df.iloc[i].matches['info']['participants'][j]['kills'])
                    tmp_lst.append(df.iloc[i].matches['info']['participants'][j]['deaths'])
                    tmp_lst.append(df.iloc[i].matches['info']['participants'][j]['assists'])
                    tmp_lst.append(df.iloc[i].matches['info']['participants'][j]['totalHeal'])
                    tmp_lst.append(df.iloc[i].matches['info']['participants'][j]['totalDamageTaken'])
                    tmp_lst.append(df.iloc[i].matches['info']['participants'][j]['totalDamageDealtToChampions'])
                    tmp_lst.append(df.iloc[i].matches['info']['participants'][j]['physicalDamageDealtToChampions'])
                    tmp_lst.append(df.iloc[i].matches['info']['participants'][j]['magicDamageDealtToChampions'])
                    tmp_lst.append(df.iloc[i].matches['info']['participants'][j]['totalDamageShieldedOnTeammates'])
                    tmp_lst.append(df.iloc[i].matches['info']['participants'][j]['totalHealsOnTeammates'])
                    tmp_lst.append(df.iloc[i].matches['info']['participants'][j]['damageSelfMitigated'])
                    
                    tmp_lst.append(df.iloc[i].matches['info']['participants'][j]['totalTimeCCDealt'])
                    tmp_lst.append(df.iloc[i].matches['info']['participants'][j]['timeCCingOthers'])
                    tmp_lst.append(df.iloc[i].matches['info']['participants'][j]['largestKillingSpree'])
                    tmp_lst.append(df.iloc[i].matches['info']['participants'][j]['goldEarned'])
                    
                # timeline_df

                    # 시작 아이템
                    starting_item = df.iloc[i].timelines["info"]["frames"][1]["events"]
                    new_starting_item = list(map(lambda x: x if x["type"] in ["ITEM_PURCHASED","ITEM_UNDO"] else 0, starting_item))
                    new_starting_item = list(filter(None, new_starting_item))
                    now_starting_item_participantId = list(filter(lambda x: x["participantId"] == participantId, new_starting_item))
                    
                    PURCHASED = []
                    UNDO = []
                    list(map(lambda x : PURCHASED.append(str(x["itemId"])) if x["type"] == "ITEM_PURCHASED" else UNDO.append(str(x["beforeId"]))\
                                                                                                   , now_starting_item_participantId))
                    for l in UNDO:
                        if l in PURCHASED:
                            PURCHASED.remove(l)
                    PURCHASED.sort()
                    tmp_lst.append("|".join(PURCHASED))

                    # 아이템빌드
                    lst = []
                    for x in range(len(df.iloc[i].timelines["info"]["frames"])):
                        lst += df.iloc[i].timelines["info"]["frames"][x]["events"]
                    new_lst = list(filter(lambda x: x["type"]  == "ITEM_PURCHASED", lst))
                    # 해당게임에서 전설/신화 아이템만 구매한  timeline
                    show_item = list(map(lambda x: x if str(x["itemId"]) in l_item else 0, new_lst))
                    show_item = list(filter(None, show_item)) # 빈값 걸러내기
                    # 현재 작업중인 participantId 의 아이템 구매 내역만 가져오기.
                    now_item_participantId = list(filter(lambda x: x["participantId"] == participantId,show_item)) 
                    # 혹시 모를 중복을 제거하고 구매했던 아이템id만 tmp_lst에 담음
                    tmp_item = []
                    for n in range(len(now_item_participantId)):
                        if now_item_participantId[n]["itemId"] not in tmp_item:
                            tmp_item.append(str(now_item_participantId[n]["itemId"]))
                    main_core = []
                    sub_core = []
                    if len(tmp_item) > 3 :
                        for y in range(len(tmp_item)):
                            if y < 3 :
                                main_core.append(tmp_item[y])
                            else:
                                sub_core.append(tmp_item[y])
                    else:
                        for z in range(len(tmp_item)):
                            if z < 3 :
                                main_core.append(tmp_item[z])
                        sub_core.append('0')
                    tmp_lst.append("|".join(main_core))
                    tmp_lst.append("|".join(sub_core))

                    # 신발
                    boots_lst = list(filter(lambda x: x["type"]  == "ITEM_PURCHASED", lst))
                    show_boots_lst = list(map(lambda x: x if str(x["itemId"]) in boots else 0, boots_lst))
                    show_boots_lst = list(filter(None, show_boots_lst))
                    try:
                        now_boots_participantId = list(filter(lambda x: x["participantId"] == participantId, show_boots_lst))[-1] # 10번은 변수로
                        tmp_lst.append(str(now_boots_participantId["itemId"]))
                    except:
                        tmp_lst.append('0')
                    

                    # 스킬 찍는 순서
                    new_skill_lst = list(filter(lambda x: x["type"]  == "SKILL_LEVEL_UP", lst))
                    now_skill_participantId = list(filter(lambda x: x["participantId"] == participantId, new_skill_lst)) # 10번은 변수로
                    skill_build= list(map(lambda x: str(x["skillSlot"]), now_skill_participantId))
                    tmp_lst.append("|".join(skill_build))

                    match_df_lst.append(tmp_lst)        
        except:
            continue    
            
    match_df = pd.DataFrame(match_df_lst,columns=['gameId','participantId','championId','championName','spell1','spell2','main_rune','sub_rune'
            ,'stat_rune','teamId','win','kills','deaths','assists','totalHeal'
           ,'totalDamageTaken','totalDamageDealtToChampions','physicalDamageDealtToChampions','magicDamageDealtToChampions'
             ,'totalDamageShieldedOnTeammates','totalHealsOnTeammates','SelfMitigatedDamage','totalTimeCCDealt','timeCCingOthers'
            ,'largestKillingSpree','goldEarned','starting_item','main_item_build','sub_item_build','boots','skill_build'])
    match_df = match_df.drop_duplicates()
    print("matches:",len(match_df))
    print("complete !")
    return match_df

# match_df db에 insert
def insert_match(df):
    df.progress_apply(lambda x : insert_m(x), axis=1)

def insert_m(row):
    query = (
        f'merge into match_aram using dual on (gameId = \'{row.gameId}\' and participantId = {row.participantId})'
        f' when not matched then'
        f' insert values(\'{row.gameId}\',{row.participantId},{row.championId},\'{row.championName}\','
        f'{row.spell1},{row.spell2},\'{row.main_rune}\',\'{row.sub_rune}\',\'{row.stat_rune}\',{row.teamId},\'{row.win}\',{row.kills},{row.deaths},'
        f'{row.assists},{row.totalHeal},'
        f'{row.totalDamageTaken},{row.totalDamageDealtToChampions},{row.physicalDamageDealtToChampions},{row.magicDamageDealtToChampions},'
        f'{row.totalDamageShieldedOnTeammates},{row.totalHealsOnTeammates},{row.SelfMitigatedDamage},{row.totalTimeCCDealt},'
        f'{row.timeCCingOthers},{row.largestKillingSpree},{row.goldEarned},'
        f'\'{row.starting_item}\',\'{row.main_item_build}\',\'{row.sub_item_build}\',\'{row.boots}\',\'{row.skill_build}\')'
            )
    sql(query)        
    return