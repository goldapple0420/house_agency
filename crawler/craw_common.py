import requests
import json
import pandas as pd
import sqlite3
import time, datetime, random
import logging
import uuid
import os, re
from fake_useragent import UserAgent
from concurrent_log_handler import ConcurrentRotatingFileHandler
import pymysql
from sqlalchemy import create_engine
from bs4 import BeautifulSoup as bs
import emoji

all_city_dict = {
            "A": "臺北市",
            "B": "臺中市",
            "C": "基隆市",
            "D": "臺南市",
            "E": "高雄市",
            "F": "新北市",
            "G": "宜蘭縣",
            "H": "桃園市",
            "I": "嘉義市",
            "J": "新竹縣",
            "K": "苗栗縣",
            "M": "南投縣",
            "N": "彰化縣",
            "O": "新竹市",
            "P": "雲林縣",
            "Q": "嘉義縣",
            "T": "屏東縣",
            "U": "花蓮縣",
            "V": "臺東縣",
            "X": "澎湖縣",
            "W": "金門縣",
            "Z": "連江縣"
            }
# 取得代理IP
def get_proxies():
    url = "https://bard.yeshome.net.tw/proxy/proxy_ip_v3/"
    data = {
        'twip': True,
        'tag_data': 'usezone'
    }
    res = requests.post(url, data=data)
    ip_txt = res.text
    j_ip_txt = json.loads(ip_txt)
    proxy = j_ip_txt['ip']
    proxies = {"http": proxy, "https": proxy}
    return proxies

# 產生新的group_key
def get_gkey():
    unique_id = uuid.uuid4()
    key = 'yh' + str(unique_id).replace('-', '')
    return key

# 將列表清單的基本資料放入自己的資料庫，以比對往後新資料
def to_sqlite(self, list_obj):
    df = pd.DataFrame(list_obj)
    con = sqlite3.connect(os.path.abspath('compare.db'))
    table_name = f'{self.name}_{time.strftime("%m%d")}'
    df.to_sql(table_name, con, if_exists='append', index=False)
    con.close()
    print(f'已將{len(list_obj)}個物件寫入SQLite資料表{table_name}')

# 連接資料庫（有要用到pandas寫入、讀取建議用此）關閉的方式：engine.dispose()
def con2sql():
    db_user = 'root'
    db_password = ''
    db_host = '127.0.0.1'
    db_name = 'housecase'
    db_data = f'mysql+mysqldb://{db_user}:{db_password}@{db_host}:3306/{db_name}?charset=utf8mb4'
    engine = create_engine(db_data)
    return engine

# 將房源資料寫入資料庫中
def data2sql(data_list, city_code):
    # 連接資料庫
    engine = con2sql()
    # 寫入
    table_name = f'jr_{city_code.lower()}'
    df = pd.DataFrame(data_list)
    df.to_sql(table_name, con=engine, if_exists='append', index=False)
    print(f'已將{len(df)}筆資料已寫入資料庫中的{table_name}')
    engine.dispose()

# 用pymysql連接資料庫（單純要下SQL指令建議用此）關閉的方式：conn.close()
def conn2pymysql():
    # 連接資料庫
    db_user = 'rita'
    db_password = 'y5416h7553'
    db_host = '192.168.1.15'
    db_name = 'housecase'
    conn = pymysql.connect(host=db_host, port=3306, db=db_name, user=db_user, password=db_password)
    return conn

# 如果這個key沒有man，要從整個group裡面找出man，請示先將這個group裡所有obj丟到一個pandas df
def FindManInObjs(find_man_objs_df):
    max_score = 1
    im_man = dict()
    for index, obj in find_man_objs_df.iterrows():
        obj_score = 0
        if obj['road'] != '':
            obj_score+=3
        if obj['situation'] != '':
            obj_score+=1
        if obj['feature'] != '':
            obj_score+=1
        if obj['pattern'] != '':
            obj_score+=3
        if obj['total_ping'] != 0:
            obj_score+=2
        if obj['building_ping'] != 0:
            obj_score+=2
        if obj['att_ping'] != 0:
            obj_score+=2
        if obj['public_ping'] != 0:
            obj_score+=2
        if obj['land_ping'] != 0:
            obj_score+=2
        if obj['house_age_v'] != 0:
            obj_score+=2
        if obj['floor'] != 0:
            obj_score+=2
        if obj['total_floor'] != 0:
            obj_score+=2
        if obj['house_num'] != 0:
            obj_score+=1
        if obj['blockto'] != '':
            obj_score+=1
        if obj['house_type'] != '':
            obj_score+=2
        if obj['manage_type'] != '':
            obj_score+=1
        if obj['manage_fee'] != '':
            obj_score+=1
        if obj['edge'] != '':
            obj_score+=1
        if obj['dark'] != '':
            obj_score+=1
        if obj['parking_type'] != '':
            obj_score+=1
        if obj['lat'] != 0:
            obj_score+=3
        if obj['lng'] != 0:
            obj_score+=3
        if obj['img_url'] != '':
            obj_score+=1
        if obj['contact_man'] != '':
            obj_score+=1
        if obj['phone'] != '':
            obj_score+=2
        if obj['brand'] != '':
            obj_score+=1
        if obj['branch'] != '':
            obj_score+=2
        if obj['community'] != '':
            obj_score+=2
        
        if obj_score > max_score:
            max_score = obj_score
            im_man = {"Index":index, "sn":obj['sn'], "group_key":obj['group_key'], "score":obj_score}
    
    # 將最終候選人的sn回傳
    return im_man


# 用「資料庫的現有資料MySQL」去跟「今天抓到的資料SQLite」做比對，找出新的案源
def is_new(self, city_code):
    # 先抓出案源資料庫，該縣市同source的資料
    table_name = f'jr_{city_code.lower()}'
    conn = con2sql()
    sql = f" SELECT source_id FROM housecase.{table_name} WHERE source = '{self.source}' ORDER BY source_id ; "
    now_ids = pd.read_sql(sql, con=conn)
    conn.dispose()

    # 再抓出今天抓取的資料
    re_conn = sqlite3.connect(os.path.abspath('compare.db'))
    now_table = f'{self.name}_{time.strftime("%m%d")}'
    city = all_city_dict[city_code]
    sql = f" SELECT distinct(source_id) FROM {now_table} WHERE city = '{city}' ORDER BY source_id ; "
    today_ids = pd.read_sql(sql, con=re_conn)
    
    # 比較
    # diff = pd.concat([today_ids, now_ids]).drop_duplicates(keep=False)
    # new_ids = diff['source_id'].tolist()
    new_ids = today_ids[~today_ids['source_id'].isin(now_ids['source_id'])]['source_id'].tolist()
    new_list = dict()
    # 有新房源的話 把新id的資料從sqlite抓出來
    if new_ids:
        print(f'{self.source}有{len(new_ids)}個新物件ID')
        print(new_ids)
        new_ids = '"' + '","'.join(new_ids) + '"'
        sql = f" SELECT * FROM {now_table} WHERE source_id in ({new_ids})"
        new_list = pd.read_sql(sql, con=re_conn).drop_duplicates(subset='source_id')
        new_list = new_list.to_dict(orient='records')
    
    re_conn.close()
    return new_list


# 用「資料庫的現有資料MySQL」去跟「今天抓到的資料SQLite」做比對，將「失效物件」的is_delete改為1
def is_del(self, city_code):
    # 先抓出案源資料庫，該縣市同source的資料
    table_name = f'jr_{city_code.lower()}'
    conn = con2sql()
    sql = f" SELECT source_id FROM housecase.{table_name} WHERE source = '{self.source}' ORDER BY source_id ; "
    now_ids = pd.read_sql(sql, con=conn)
    conn.dispose()

    # 再抓出今天抓取的資料
    re_conn = sqlite3.connect(os.path.abspath('compare.db'))
    now_table = f'{self.name}_{time.strftime("%m%d")}'
    city = all_city_dict[city_code]
    sql = f" SELECT source_id FROM {now_table} WHERE city = '{city}' ORDER BY source_id ; "
    today_ids = pd.read_sql(sql, con=re_conn)
    re_conn.close()
    
    # 比較
    # diff = pd.concat([now_ids, today_ids]).drop_duplicates(keep=False)
    # del_ids = diff['source_id'].tolist()
    del_ids = now_ids[~now_ids['source_id'].isin(today_ids['source_id'])]['source_id'].tolist()
    
    # 失效物件的處理
    if del_ids:
        print(f'找到{self.source} {len(del_ids)}個已失效的物件ID')
        print(del_ids)
        del_ids = '"' + '","'.join(del_ids) + '"'
        update_time = time.strftime("%Y-%m-%d %H:%M:%S")
        
        conn = conn2pymysql()
        cursor = conn.cursor()
        # 更新資料庫的失效物件，is_delete改為1，及更新時間
        sql = f" UPDATE housecase.{table_name} SET is_delete = 1, update_time = '{update_time}' WHERE source = '{self.source}' AND source_id in ({del_ids}); "
        cursor.execute(sql)
        conn.commit()

        # 把失效的物件移到下市的表格
        del_table = f'jz_{city_code.lower()}'
        # sql = f" INSERT INTO housecase.{del_table} SELECT * FROM `housecase`.{table_name} WHERE source = '{self.source}' AND is_delete = 1; "
        # sn不能選到 所以要選其他所有欄位
        sql = f"""
        INSERT INTO housecase.{del_table} (`source`,`source_id`,`subject`,`city`,`area`,`road`,`address`,`situation`,
        `total`,`price_ave`,`feature`,`pattern`,`pattern1`,`total_ping`,`building_ping`,`att_ping`,`public_ping`,`land_ping`,
        `house_age`,`house_age_v`,`floor_web`,`floor`,`total_floor`,`house_num`,`blockto`,`house_type`,`manage_type`,`manage_fee`,
        `edge`,`dark`,`parking_type`,`lat`,`lng`,`link`,`img_url`,`contact_type`,`contact_man`,`phone`,`brand`,`branch`,`company`,
        `price_renew`,`insert_time`,`update_time`,`community`,`mrt`,`group_man`,`group_key`,`group_record`,`history`,`address_cal`,
        `is_delete`,`is_hidden`)
        
        SELECT `source`,`source_id`,`subject`,`city`,`area`,`road`,`address`,`situation`,
        `total`,`price_ave`,`feature`,`pattern`,`pattern1`,`total_ping`,`building_ping`,`att_ping`,`public_ping`,`land_ping`,
        `house_age`,`house_age_v`,`floor_web`,`floor`,`total_floor`,`house_num`,`blockto`,`house_type`,`manage_type`,`manage_fee`,
        `edge`,`dark`,`parking_type`,`lat`,`lng`,`link`,`img_url`,`contact_type`,`contact_man`,`phone`,`brand`,`branch`,`company`,
        `price_renew`,`insert_time`,`update_time`,`community`,`mrt`,`group_man`,`group_key`,`group_record`,`history`,`address_cal`,
        `is_delete`,`is_hidden` 
        FROM `housecase`.{table_name} WHERE source = '{self.source}' AND is_delete = 1; 
        """
        cursor.execute(sql)
        sql = f" DELETE FROM housecase.{table_name} WHERE source = '{self.source}' AND is_delete = 1; "
        cursor.execute(sql)
        conn.commit()
        print(f'已將{self.source}失效物件移至下市表格{del_table}')
        # 抓出失效物件中，是group_man的
        pd_conn = con2sql()
        sql = f" SELECT group_key as group_key FROM housecase.{del_table} WHERE source = '{self.source}' AND is_delete = 1 AND group_man = 1; "
        no_man_keys = pd.read_sql(sql, con=pd_conn)
        for gkey in no_man_keys['group_key']:
            # 防止先前其他錯誤資料，先確定key裡沒有man
            sql = f" UPDATE housecase.{table_name} SET group_man = 0 WHERE group_key = '{gkey}'; "
            cursor.execute(sql)
            conn.commit()
            # 先把這個group中的有效物件抓出來，丟進去找新的man
            sql = f" SELECT * FROM housecase.{table_name} WHERE group_key = '{gkey}'; "
            find_man_objs = pd.read_sql(sql, con=pd_conn)
            win_obj = FindManInObjs(find_man_objs)
            if win_obj:
                win_sn = win_obj['sn']
                sql = f" UPDATE housecase.{table_name} SET group_man = 1 WHERE sn = '{win_sn}' "
                cursor.execute(sql)
                print(f'{gkey}已選出新的group_man! SN:{win_sn}')
            else:
                print(f'{gkey} 中無其他物件！')
        conn.commit()
        conn.close()
        pd_conn.dispose()
    else:
        print(f'{table_name}資料表中無{self.source}失效物件！')

# 找出有價格更新的物件，更新total價格、price_ave平均價格、price_renew、history、update_time
def is_update(self, city_code):
    # 先抓出案源資料庫，該縣市同source的資料
    table_name = f'jr_{city_code.lower()}'
    conn = con2sql()
    sql = f" SELECT sn, source_id, total, history, subject, link FROM housecase.{table_name} WHERE source = '{self.source}' ORDER BY source_id ; "
    now_info = pd.read_sql(sql, con=conn)
    # print(now_info)
    conn.dispose()

    # 再抓出今天抓取的資料
    re_conn = sqlite3.connect(os.path.abspath('compare.db'))
    now_table = f'{self.name}_{time.strftime("%m%d")}'
    city = all_city_dict[city_code]
    sql = f"""
    SELECT source_id, re_price
    FROM {now_table} WHERE city = '{city}' ORDER BY source_id ; 
    """
    today_info = pd.read_sql(sql, con=re_conn)
    # print(today_info)
    re_conn.close()
    
    # 合併兩個表格，根據'source_id'欄位進行合併
    merged_df = pd.merge(today_info, now_info, on='source_id')

    # 篩選出total不一致的資料 ##!兩邊資料的格式都要是int，不能是文字或float
    new_info = merged_df[merged_df['re_price'] != merged_df['total']]

    # 將MYSQL舊價格的total欄位刪除
    new_info = new_info.drop(columns=['total'])
    # 重新命名新抓到的價格re_price欄位為total
    new_info = new_info.rename(columns={'re_price': 'total'})

    # 將更新的資料寫入資料庫
    conn = conn2pymysql()
    cursor = conn.cursor()
    for index, obj in new_info.iterrows():
        new_total = obj['total']
        source_id = obj['source_id']
        subject = obj['subject']
        link = obj['link']
        sn = obj['sn']
        insert_time = time.strftime("%Y-%m-%d")
        add_his = ',{' + f'"source":"{self.source}","source_id":"{source_id}","total":"{new_total}","subject":"{subject}","insert_time":"{insert_time}","link":"{link}"' + '}]'
        now_his = obj['history'].rstrip(']')
        history = now_his + add_his
        update_time = time.strftime("%Y-%m-%d %H:%M:%S")
        # 一般建物計算方式
        sql = f""" 
        UPDATE housecase.{table_name} 
        SET total = {new_total}, price_renew = 1, price_ave = round(total/total_ping, 2), history = '{history}', update_time = '{update_time}'
        WHERE sn = '{sn}' AND total_ping !=0 ;
        """
        cursor.execute(sql)
        # 土地計算方式
        sql = f""" 
        UPDATE housecase.{table_name} 
        SET total = {new_total}, price_renew = 1, price_ave = round(total/land_ping, 2), history = '{history}', update_time = '{update_time}'
        WHERE sn = '{sn}' AND total_ping =0 AND land_ping!=0 ;
        """
        cursor.execute(sql)
        # 資料不完全的物件平均單價寫0
        sql = f""" 
        UPDATE housecase.{table_name} 
        SET total = {new_total}, price_renew = 1, price_ave = 0, history = '{history}', update_time = '{update_time}'
        WHERE sn = '{sn}' AND total_ping =0 AND land_ping=0 ;
        """
        cursor.execute(sql)
        print(f'{table_name}表 {self.source} ID:{source_id} 價格已更新至資料庫')
    conn.commit()
    conn.close()
    print('更新物件資料結束')

# 遇錯誤時寫入log紀錄 from web_scraping/web_scraping/logger.py
class WriteLogger:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(threadName)s]: %(message)s')
    logger = logging.getLogger(__name__)
    # 檔案輸出格式
    formatter = logging.Formatter('%(asctime)s [%(threadName)s]: %(message)s')

    error_logfile = os.path.abspath("logs/error.log")

    console_error = ConcurrentRotatingFileHandler(error_logfile, 'a', 512*1024*10, 10)
    console_error.setLevel(logging.ERROR)
    console_error.setFormatter(formatter)
    logging.getLogger('').addHandler(console_error)

    def getLoggers(self):
        return self.logger

# 計算建物比對Group的分數
def group_score(obj, man):
    score = 0
    # 格局拆成房數、廳數、衛浴數
    match = re.findall(r'(\d+)房', obj['pattern'])
    obj_room = int(match[0]) if match else 0
    match = re.findall(r'(\d+)房', man['pattern'])
    man_room = int(match[0]) if match else 0
    
    match = re.findall(r'(\d+)廳', obj['pattern'])
    obj_hall = int(match[0]) if match else 0
    match = re.findall(r'(\d+)廳', man['pattern'])
    man_hall = int(match[0]) if match else 0

    match = re.findall(r'(\d+)衛', obj['pattern'])
    obj_toilet = int(match[0]) if match else 0
    match = re.findall(r'(\d+)衛', man['pattern'])
    man_toilet = int(match[0]) if match else 0
    
    # 東森房屋僅提供主建物+附屬建物坪數，沒有個別提供主建物跟附屬建物
    if obj['source'] == '東森房屋':
        man['building_ping'] = man['building_ping'] + man['att_ping']

    # 非土地類的計算方式，滿分55分，得分高於35分，則將物件加入這group，若與多個group分數高於35，則取最高分的group加入
    if obj['road'] != '' and man['road'] != '':
        score+=5
    if obj['address'] == man['address']:
        score+=5
    if obj_room == man_room:
        score+=2
    if obj_hall == man_hall:
        score+=2
    if obj_toilet == man_toilet:
        score+=2
    if obj['total_ping'] == man['total_ping']:
        score+=4
    if obj['building_ping'] == man['building_ping']:
        score+=3
    if obj['att_ping'] == man['att_ping']:
        score+=3
    if obj['public_ping'] == man['public_ping']:
        score+=3
    if obj['land_ping'] == man['land_ping']:
        score+=4
    if obj['house_age_v'] == man['house_age_v']:
        score+=3
    if (man['house_age_v']-2 <= obj['house_age_v']) or (obj['house_age_v'] <= man['house_age_v']+2):
        score+=1
    if obj['floor'] == man['floor']:
        score+=3
    if obj['total_floor'] == man['total_floor']:
        score+=3
    if (obj['lat'] == man['lat']) and (obj['lng'] == man['lng']):
        score+=8
    if obj['house_type'] == man['house_type']:
        score+=4
    
    #如果房屋類型是大樓、電梯大樓、華廈，floor一定要一樣，怕是不同物件，但若是同社區，格局坪數地址位置可能都一樣
    if '大樓' in obj['house_type'] or '華夏' in obj['house_type']:
        if obj['floor'] != man['floor']:
            score = 0
    
    return score

# 計算此物件能不能成為group_man（資料完整度有沒有贏過目前的group_man）
def is_man(obj, man):
    obj_score = 0
    man_score = 0

    # 算分數方式寫這
    if '地' not in obj['house_type']:
        # 先算要來比賽當man的Group新成員
        if obj['road'] != '':
            obj_score+=3
        if obj['situation'] != '':
            obj_score+=1
        if obj['feature'] != '':
            obj_score+=1
        if obj['pattern'] != '':
            obj_score+=3
        if obj['total_ping'] != 0:
            obj_score+=2
        if obj['building_ping'] != 0:
            obj_score+=2
        if obj['att_ping'] != 0:
            obj_score+=2
        if obj['public_ping'] != 0:
            obj_score+=2
        if obj['land_ping'] != 0:
            obj_score+=2
        if obj['house_age_v'] != 0:
            obj_score+=2
        if obj['floor'] != 0:
            obj_score+=2
        if obj['total_floor'] != 0:
            obj_score+=2
        if obj['house_num'] != 0:
            obj_score+=1
        if obj['blockto'] != '':
            obj_score+=1
        if obj['house_type'] != '':
            obj_score+=2
        if obj['manage_type'] != '':
            obj_score+=1
        if obj['manage_fee'] != '':
            obj_score+=1
        if obj['edge'] != '':
            obj_score+=1
        if obj['dark'] != '':
            obj_score+=1
        if obj['parking_type'] != '':
            obj_score+=1
        if obj['lat'] != 0:
            obj_score+=3
        if obj['lng'] != 0:
            obj_score+=3
        if obj['img_url'] != '':
            obj_score+=1
        if obj['contact_man'] != '':
            obj_score+=1
        if obj['phone'] != '':
            obj_score+=2
        if obj['brand'] != '':
            obj_score+=1
        if obj['branch'] != '':
            obj_score+=2
        if obj['community'] != '':
            obj_score+=2
        
        # 再來算目前的Group man的分數
        if man['road'] != '':
            man_score+=3
        if man['situation'] != '':
            man_score+=1
        if man['feature'] != '':
            man_score+=1
        if man['pattern'] != '':
            man_score+=3
        if man['total_ping'] != 0:
            man_score+=2
        if man['building_ping'] != 0:
            man_score+=2
        if man['att_ping'] != 0:
            man_score+=2
        if man['public_ping'] != 0:
            man_score+=2
        if man['land_ping'] != 0:
            man_score+=2
        if man['house_age_v'] != 0:
            man_score+=2
        if man['floor'] != 0:
            man_score+=2
        if man['total_floor'] != 0:
            man_score+=2
        if man['house_num'] != 0:
            man_score+=1
        if man['blockto'] != '':
            man_score+=1
        if man['house_type'] != '':
            man_score+=2
        if man['manage_type'] != '':
            man_score+=1
        if man['manage_fee'] != '':
            man_score+=1
        if man['edge'] != '':
            man_score+=1
        if man['dark'] != '':
            man_score+=1
        if man['parking_type'] != '':
            man_score+=1
        if man['lat'] != 0:
            man_score+=3
        if man['lng'] != 0:
            man_score+=3
        if man['img_url'] != '':
            man_score+=1
        if man['contact_man'] != '':
            man_score+=1
        if man['phone'] != '':
            man_score+=2
        if man['brand'] != '':
            man_score+=1
        if man['branch'] != '':
            man_score+=2
        if man['community'] != '':
            obj_score+=2
    # 土地類的計算方式
    else:
        # 先算要來比賽當man的Group新成員
        if obj['road'] != '':
            obj_score+=5
        if obj['situation'] != '':
            obj_score+=1
        if obj['feature'] != '':
            obj_score+=1
        if obj['total_ping'] != 0:
            obj_score+=3
        if obj['land_ping'] != 0:
            obj_score+=4
        if obj['lat'] != 0:
            obj_score+=4
        if obj['lng'] != 0:
            obj_score+=4
        if obj['img_url'] != '':
            obj_score+=1
        if obj['contact_man'] != '':
            obj_score+=1
        if obj['phone'] != '':
            obj_score+=2
        if obj['brand'] != '':
            obj_score+=1
        if obj['branch'] != '':
            obj_score+=2
        
        # 再來算目前的Group man的分數
        if man['road'] != '':
            man_score+=5
        if man['situation'] != '':
            man_score+=1
        if man['feature'] != '':
            man_score+=1
        if man['total_ping'] != 0:
            man_score+=3
        if man['land_ping'] != 0:
            man_score+=4
        if man['lat'] != 0:
            man_score+=4
        if man['lng'] != 0:
            man_score+=4
        if man['img_url'] != '':
            man_score+=1
        if man['contact_man'] != '':
            man_score+=1
        if man['phone'] != '':
            man_score+=2
        if man['brand'] != '':
            man_score+=1
        if man['branch'] != '':
            man_score+=2

    # 0: group_man不變 1:此obj贏過原本的group_man，當選新的group_man
    if obj_score <= man_score:
        return 0
    else:
        return 1

# 幫房源找到group
def find_group(self, city_code):
    pd_conn = con2sql()
    conn = conn2pymysql()
    cursor = conn.cursor()
    table_name = f'jr_{city_code.lower()}'
    # 先找出無家可歸的孩子們
    sql = f" SELECT * FROM housecase.{table_name} WHERE group_key = '' AND source = '{self.source}' "
    no_group = pd.read_sql(sql, con=pd_conn)

    # 從案源資料庫抓「同行政區」、「價格正負10%」、「road一樣或為空」、「是group_man」的物件出來
    for index, obj in no_group.iterrows():
        print(f'{self.source} Source_id:' + obj['source_id'] + ' 開始找Group')
        area = obj['area']
        road = obj['road']
        house_type = obj['house_type']
        min_price = obj['total'] * 0.9
        max_price = obj['total'] * 1.1
        pass2_sn = [] # 第二階段通過的GROUP列表
        # 土地以外的比對group方式
        if '地' not in house_type:
            # 作為第一階段的篩選
            sql = f"""
            SELECT * FROM housecase.{table_name} 
            WHERE area = '{area}' AND (road = '{road}' or road = '') 
            AND total BETWEEN {min_price} AND {max_price} AND group_man = 1 ;
            """
            pass1_df = pd.read_sql(sql, con=pd_conn)
            for index, man in pass1_df.iterrows():
                score = group_score(obj, man)
                compare_key = man['group_key']
                # print(f'比對Index{index} Group_Key:{compare_key}獲得{score}分')
                if score >= 35:
                    pass2_sn.append({"key":compare_key, "score":score})
        # 土地的計算分數方式
        else:
            min_land_ping = obj['land_ping'] - 2
            max_land_ping = obj['land_ping'] + 2
            check_cloumns = ['total', 'road', 'lat', 'lng', 'land_ping', 'total_ping']

            sql = f" SELECT *  FROM housecase.{table_name} WHERE area = '{area}' AND (road = '{road}' OR road = '') AND (land_ping BETWEEN {min_land_ping} AND {max_land_ping} OR total_ping BETWEEN {min_land_ping} AND {max_land_ping}) AND total BETWEEN {min_price} AND {max_price} AND group_man = 1;"
            pass1_df = pd.read_sql(sql, con=pd_conn)

            for index, row in pass1_df.iterrows():
                point = 0
                if obj['land_ping'] == row['total_ping'] or obj['total_ping'] == row['land_ping']:
                    point += 2
                for column in check_cloumns:
                    if obj[column] == row[column]:
                        point += 1
                        if column in ['lat', 'lng', 'land_ping'] and obj[column]!=0:
                            point += 2
                compare_key = row['group_key']
                # print(f'比對Index{index} Group_Key:{compare_key}獲得{point}分')
                pass2_sn.append({"key":compare_key, "score":point})

        # 沒找到group的，自組一個新group，並成為group_man
        if len(pass2_sn) == 0:
            print('比對不到Group,自組新key')
            obj_key = get_gkey()
            obj_man = 1
            old_record = '{'
        elif len(pass2_sn) == 1:
            obj_key = pass2_sn[0]['key']
            sql = f" SELECT * FROM housecase.{table_name} WHERE group_key = '{obj_key}' AND group_man = 1 "
            man_df = pd.read_sql(sql, pd_conn)
            man = pd.Series(man_df.iloc[0,:])
            old_record = man['group_record']
            if old_record == '[]':
                old_record = '{'
            else:
                old_record = old_record[:-1] + ','
            obj_man = is_man(obj, man)
            if obj_man == 1:
                # 先把這個key原本的man改0，再寫入這筆
                sql = f" UPDATE housecase.{table_name} SET group_man = 0 WHERE group_key = '{obj_key}' AND group_man = 1 "
                cursor.execute(sql)
                conn.commit()
        else: # 有多個找最高分
            obj_key = max(pass2_sn, key=lambda x: x['score'])['key']
            sql = f" SELECT * FROM housecase.{table_name} WHERE group_key = '{obj_key}' AND group_man = 1 "
            man_df = pd.read_sql(sql, pd_conn)
            man = pd.Series(man_df.iloc[0,:])
            old_record = man['group_record']
            if old_record == '[]':
                old_record = '{'
            else:
                old_record = old_record[:-1] + ','
            obj_man = is_man(obj, man)
            if obj_man == 1:
                sql = f" UPDATE housecase.{table_name} SET group_man = 0 WHERE group_key = '{obj_key}' AND group_man = 1 "
                cursor.execute(sql)
                conn.commit()
        
        print(f'加入group囉！GROUP KEY:{obj_key}')
        source = obj['source']
        date = time.strftime("%Y-%m-%d")
        obj_sn = obj['sn']
        link = obj['link']
        price = obj['total']
        record =f'"{source}":' + '{' + f'"public":"{date}","sn":"{obj_sn}","wbsite":"{link}","price":"{price}"' + '}}'
        new_record = old_record + record
        # print(new_record)
        sql = f" UPDATE housecase.{table_name} SET group_man = {obj_man} , group_key = '{obj_key}' WHERE sn = {obj_sn} ; "
        cursor.execute(sql)
        # 更新此group內所有物件的group_record、address_cal
        sql = f" SELECT address_cal FROM housecase.{table_name} WHERE group_man = 1 AND group_key = '{obj_key}' ; "
        cursor.execute(sql)
        copy_add = cursor.fetchone()[0].replace("'",'"')
        sql = f" UPDATE housecase.{table_name} SET group_record = '{new_record}', address_cal = '{copy_add}' WHERE group_key = '{obj_key}'; "
        cursor.execute(sql)
        conn.commit()

    pd_conn.dispose()
    conn.close()


def find_possible_addrs(self, city_code):
    pd_conn = con2sql()
    conn = conn2pymysql()
    cursor = conn.cursor()
    table_name = f'jr_{city_code.lower()}'
    sql = f" SELECT * FROM housecase.{table_name} WHERE source = '{self.source}' AND address_cal = '' AND group_man = 1 ;"
    no_addrs = pd.read_sql(sql, con=pd_conn)
    pd_conn.dispose()
    for index, obj in no_addrs.iterrows():
        if obj['total_ping'] == 0:
            min_total_ping, max_total_ping = '',''
        else:
            min_total_ping = obj['total_ping'] - 0.2
            max_total_ping = obj['total_ping'] + 0.2
        if obj['building_ping'] == 0:
            min_main_ping, max_main_ping = '',''
        else:
            min_main_ping = obj['building_ping'] - 0.2
            max_main_ping = obj['building_ping'] + 0.2
        if obj['att_ping'] == 0:
            min_attach_ping, max_attach_ping = '',''
        else:
            min_attach_ping = obj['att_ping'] - 0.2
            max_attach_ping = obj['att_ping'] + 0.2
        if obj['public_ping'] == 0:
            min_public_ping, max_public_ping = '',''
        else:
            min_public_ping = obj['public_ping'] - 0.2
            max_public_ping = obj['public_ping'] + 0.2
        if obj['floor'] < 0:
            min_level, max_level = '',''
        else:
            min_level = obj['floor']
            max_level = obj['floor']
        if obj['total_floor'] < 0:
            min_total_level, max_total_level = '',''
        else:
            min_total_level = obj['total_floor']
            max_total_level = obj['total_floor']
        min_age = obj['house_age_v'] - 1.5
        if min_age < 0:
            min_age = ''
        max_age = obj['house_age_v'] + 1.5
        group_key = obj['group_key']
        user_agent = UserAgent()
        header = {'user-agent':user_agent.random}
        api_url = 'http://land-data.yeshome.net.tw/build_data/find_possible_addrs_sc/'
        params = {
            'token':'7079e8b7-a5a3-415d-ab66-38f8cf5cdcbc',
            'city_name': obj['city'],
            'area_name': obj['area'],
            'road': obj['road'],
            'min_total_ping': min_total_ping,
            'max_total_ping': max_total_ping,
            'min_main_ping': min_main_ping,
            'max_main_ping': max_main_ping,
            'min_attach_ping': min_attach_ping,
            'max_attach_ping': max_attach_ping,
            'min_public_ping': min_public_ping,
            'max_public_ping': max_public_ping,
            'min_level': min_level,
            'max_level': max_level,
            'min_total_level': min_total_level,
            'max_total_level': max_total_level,
            'min_age': min_age,
            'max_age': max_age
        }
        time.sleep(random.uniform(2,7))
        api_res = requests.get(api_url, params=params, headers=header)
        result = bs(api_res.text, 'lxml').find('p').text.replace("'",'"')
        sql = f" UPDATE housecase.{table_name} SET address_cal = '{result}' WHERE group_key = '{group_key}';  "
        cursor.execute(sql)
        conn.commit()
        print(f'Group比對可能地址完成！{group_key}')
    conn.close()

def clean_data(data):
    logger = WriteLogger().getLoggers()
    
    # 確認資料型態
    if type(data['total']) != int:
        try:
            data['total'] = int(float(data['total']))
        except Exception as e:
            logger.error("MySQLdb Error : DATATYPE IS WRONG! TOTAL:", data['total'],"Error:", e)
            data['total'] = 0
    if type(data['price_ave']) != float:
        try:
            data['price_ave'] = float(data['price_ave'])
        except Exception as e:
            logger.error("MySQLdb Error : DATATYPE IS WRONG! PRICE_AVE:", data['prcie_ave'],"Error:", e)
            data['price_ave'] = 0
    if type(data['total_ping']) != float:
        try:
            data['total_ping'] = float(data['total_ping'])
        except Exception as e:
            logger.error("MySQLdb Error : DATATYPE IS WRONG! TOTAL_PING:", data['total_ping'],"Error:", e)
            data['total_ping'] = 0
    if type(data['building_ping']) != float:
        try:
            data['building_ping'] = float(data['building_ping'])
        except Exception as e:
            logger.error("MySQLdb Error : DATATYPE IS WRONG! BUILDING_PING:", data['building_ping'],"Error:", e)
            data['building_ping'] = 0
    if type(data['att_ping']) != float:
        try:
            data['att_ping'] = float(data['att_ping'])
        except Exception as e:
            logger.error("MySQLdb Error : DATATYPE IS WRONG! ATT_PING:", data['att_ping'],"Error:", e)
            data['att_ping'] = 0
    if type(data['public_ping']) != float:
        try:
            data['public_ping'] = float(data['public_ping'])
        except Exception as e:
            logger.error("MySQLdb Error : DATATYPE IS WRONG! PUBLIC_PING:", data['public_ping'],"Error:", e)
            data['public_ping'] = 0
    if type(data['land_ping']) != float:
        try:
            data['land_ping'] = float(data['land_ping'])
        except Exception as e:
            logger.error("MySQLdb Error : DATATYPE IS WRONG! LAND_PING:", data['land_ping'],"Error:", e)
            data['land_ping'] = 0
    if type(data['house_age']) != str:
        try:
            data['house_age'] = str(data['house_age'])
        except Exception as e:
            logger.error("MySQLdb Error : DATATYPE IS WRONG! HOUSE_AGE:", data['house_age'],"Error:", e)
            data['house_age'] = ''
    if type(data['house_age_v']) != float:
        try:
            data['house_age_v'] = float(data['house_age_v'])
        except Exception as e:
            logger.error("MySQLdb Error : DATATYPE IS WRONG! HOUSE_AGE_V:", data['house_age_v'],"Error:", e)
            data['house_age_v'] = -1
    if type(data['floor_web']) != str:
        try:
            data['floor_web'] = str(data['floor_web'])
        except Exception as e:
            logger.error("MySQLdb Error : DATATYPE IS WRONG! FLOOR_WEB:", data['floor_web'],"Error:", e)
            data['floor_web'] = ''
    if type(data['floor']) != int:
        try:
            data['floor'] = int(data['floor'])
        except Exception as e:
            logger.error("MySQLdb Error : DATATYPE IS WRONG! FLOOR:", data['floor'],"Error:", e)
            data['floor'] = -1
    if type(data['total_floor']) != int:
        try:
            data['total_floor'] = int(data['total_floor'])
        except TypeError as e:
            logger.error("MySQLdb Error : DATATYPE IS WRONG! TOTAL_FLOOR:", data['total_floor'],"Error:", e)
            data['total_floor'] = -1
    if type(data['lat']) != float:
        try:
            data['lat'] = float(data['lat'])
        except Exception as e:
            logger.error("MySQLdb Error : DATATYPE IS WRONG! LAT:", data['lat'],"Error:", e)
            data['lat'] = 0
    if type(data['lng']) != float:
        try:
            data['lng'] = float(data['lng'])
        except Exception as e:
            logger.error("MySQLdb Error : DATATYPE IS WRONG! LNG:", data['lng'],"Error:", e)
            data['lng'] = 0

    # 確認資料長度
    MAX_SOURCE_ID_LEN = 120
    MAX_SUBJCT_LEN = 40
    MAX_CITY_LEN = 10
    MAX_AREA_LEN = 10
    MAX_ROAD_LEN = 20
    MAX_ADDRESS_LEN = 40
    MAX_SITUATION_LEN = 60
    MAX_TOTAL = 99999999999
    MAX_FEATURE_LEN = 255
    MAX_PATTERN_LEN = 30
    MAX_PATTERN1_LEN = 30
    MAX_HOUSE_AGE_LEN = 10
    MAX_FLOOR_WEB_LEN = 30
    MAX_FLOOR = 99999
    MAX_TOTAL_FLOOR= 99999
    MAX_HOUSE_NUM_LEN = 30
    MAX_BLOCKTO_LEN = 30
    MAX_HOUSE_TYPE_LEN = 10
    MAX_MANAGE_TYPE_LEN = 16
    MAX_MANAGE_FEE_LEN = 16
    MAX_EDGE_LEN = 10
    MAX_DARK_LEN = 10
    MAX_PARKING_TYPE_LEN = 16
    MAX_LINK_LEN = 100
    MAX_CONTACT_TYPE_LEN = 10
    MAX_CONTACT_MAN_LEN = 20
    MAX_PHONE_LEN = 20
    MAX_BRAND_LEN = 40
    MAX_BRANCH_LEN = 40
    MAX_COMPANY_LEN = 40
    MAX_COMMUNITY_LEN = 30
    
    if len(data['source_id']) > MAX_SOURCE_ID_LEN:
        logger.error("MySQLdb Error : DATA TOO LONG! "+data['source']+" ID:"+data['soucre_id'])
        data['source_id'] = data['source_id'][:MAX_SOURCE_ID_LEN]
    if len(data['subject']) > MAX_SUBJCT_LEN:
        data['subject'] = data['subject'][:MAX_SUBJCT_LEN]
    if len(data['city']) > MAX_CITY_LEN:
        logger.error("MySQLdb Error : DATA TOO LONG! "+data['source']+" ID:"+data['soucre_id']+" CITY:"+data['city'])
        data['city'] = data['city'][:MAX_CITY_LEN]
    if len(data['area']) > MAX_AREA_LEN:
        data['area'] = data['area'][:MAX_AREA_LEN]
    if len(data['road']) > MAX_ROAD_LEN:
        data['road'] = data['road'][:MAX_ROAD_LEN]
    if len(data['address']) > MAX_ADDRESS_LEN:
        data['address'] = data['address'][:MAX_ADDRESS_LEN]
    if len(data['situation']) > MAX_SITUATION_LEN:
        data['situation'] = data['situation'][:MAX_SITUATION_LEN]
    if data['total'] > MAX_TOTAL:
        logger.error("MySQLdb Error : DATA TOO LONG! "+data['source']+" ID:"+data['soucre_id']+" TOTAL:", data['total'])
        data['total'] = 0
    if len(data['feature']) > MAX_FEATURE_LEN:
        data['feature'] = data['feature'][:MAX_FEATURE_LEN]
    if len(data['pattern']) > MAX_PATTERN_LEN:
        data['pattern'] = data['pattern'][:MAX_PATTERN_LEN]
    if len(data['pattern1']) > MAX_PATTERN1_LEN:
        data['pattern1'] = data['pattern1'][:MAX_PATTERN1_LEN]
    if len(data['house_age']) > MAX_HOUSE_AGE_LEN:
        data['house_age'] = data['house_age'][:MAX_HOUSE_AGE_LEN]
    if len(data['floor_web']) > MAX_FLOOR_WEB_LEN:
        data['floor_web'] = data['floor_web'][:MAX_FLOOR_WEB_LEN]
    if data['floor'] > MAX_FLOOR:
        logger.error("MySQLdb Error : DATA TOO LONG! "+data['source']+" ID:"+data['soucre_id']+" TOTAL:", data['floor'])
        data['floor'] = -1
    if data['total_floor'] > MAX_TOTAL_FLOOR:
        logger.error("MySQLdb Error : DATA TOO LONG! "+data['source']+" ID:"+data['soucre_id']+" TOTAL:", data['total_floor'])
        data['total_floor'] = -1
    if len(data['house_num']) > MAX_HOUSE_NUM_LEN:
        data['house_num'] = data['house_num'][:MAX_HOUSE_NUM_LEN]
    if len(data['blockto']) > MAX_BLOCKTO_LEN:
        data['blockto'] = data['blockto'][:MAX_BLOCKTO_LEN]
    if len(data['house_type']) > MAX_HOUSE_TYPE_LEN:
        data['house_type'] = data['house_type'][:MAX_HOUSE_TYPE_LEN]
    if len(data['manage_type']) > MAX_MANAGE_TYPE_LEN:
        data['manage_type'] = data['manage_type'][:MAX_MANAGE_TYPE_LEN]
    if len(data['manage_fee']) > MAX_MANAGE_FEE_LEN:
        data['manage_fee'] = data['manage_fee'][:MAX_MANAGE_FEE_LEN]
    if len(data['edge']) > MAX_EDGE_LEN:
        data['edge'] = data['edge'][:MAX_EDGE_LEN]
    if len(data['dark']) > MAX_DARK_LEN:
        data['dark'] = data['dark'][:MAX_DARK_LEN]
    if len(data['parking_type']) > MAX_PARKING_TYPE_LEN:
        data['parking_type'] = data['parking_type'][:MAX_PARKING_TYPE_LEN]
    if len(data['link']) > MAX_LINK_LEN:
        logger.error("MySQLdb Error : DATA TOO LONG! "+data['source']+" ID:"+data['soucre_id']+" LINK:"+data['link'])
        data['link'] = data['link'][:MAX_LINK_LEN]
    if len(data['contact_type']) > MAX_CONTACT_TYPE_LEN:
        data['contact_type'] = data['contact_type'][:MAX_CONTACT_TYPE_LEN]
    if len(data['contact_man']) > MAX_CONTACT_MAN_LEN:
        data['contact_man'] = data['contact_man'][:MAX_CONTACT_MAN_LEN]
    if len(data['phone']) > MAX_PHONE_LEN:
        data['phone'] = data['phone'][:MAX_PHONE_LEN]
    if len(data['brand']) > MAX_BRAND_LEN:
        data['brand'] = data['brand'][:MAX_BRAND_LEN]
    if len(data['branch']) > MAX_BRANCH_LEN:
        data['branch'] = data['branch'][:MAX_BRANCH_LEN]
    if len(data['company']) > MAX_COMPANY_LEN:
        data['company'] = data['company'][:MAX_COMPANY_LEN]
    if len(data['community']) > MAX_COMMUNITY_LEN:
        data['community'] = data['community'][:MAX_COMMUNITY_LEN]
    
    # 將表情符號拿掉
    emoji_pattern = re.compile("["
            u"\U0001F600-\U0001F64F" # emoticons
            u"\U0001F300-\U0001F5FF" # symbols & pictographs
            u"\U0001F680-\U0001F6FF" # transport & map symbols
            u"\U0001F1E0-\U0001F1FF" # flags (iOS)
            u"\U00100503"
            u"\U00100119"
            u"\U0010ffff"
            "]+", flags=re.UNICODE)
    data['subject'] = emoji_pattern.sub('', data['subject'])
    data['subject'] = emoji.replace_emoji(data['subject'], replace='')
    data['feature'] = emoji_pattern.sub('', data['feature'])
    data['feature'] = emoji.replace_emoji(data['feature'], replace='')
    data['contact_man'] = emoji_pattern.sub('', data['contact_man'])
    data['phone'] = emoji_pattern.sub('', data['phone'])
    data['brand'] = emoji_pattern.sub('', data['brand'])
    data['branch'] = emoji_pattern.sub('', data['branch'])
    data['company'] = emoji_pattern.sub('', data['company'])
    data['community'] = emoji_pattern.sub('', data['community'])
    data['history'] = emoji.replace_emoji(data['history'], replace='')

    return data