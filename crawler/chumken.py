import requests
from bs4 import BeautifulSoup
import urllib.request
import re
import time
import pandas as pd
from fake_useragent import UserAgent
from crawler.craw_common import *

class Chumken:
    def __init__(self):
        self.name = 'chumken'
        self.source = '春耕不動產'
        self.user_agent = UserAgent()
        self.logger = WriteLogger().getLoggers()
        self.all_city = {
        "A": {"city_c": "臺北市", "city_n": "3"},
        "B": {"city_c": "臺中市", "city_n": "9"},
        "C": {"city_c": "基隆市", "city_n": "2"},
        "D": {"city_c": "臺南市", "city_n": "16"},
        "E": {"city_c": "高雄市", "city_n": "18"},
        "F": {"city_c": "新北市", "city_n": "4"},
        "G": {"city_c": "宜蘭縣", "city_n": "1"},
        "H": {"city_c": "桃園市", "city_n": "5"},
        "I": {"city_c": "嘉義市", "city_n": "14"},
        "J": {"city_c": "新竹縣", "city_n": "7"},
        "K": {"city_c": "苗栗縣", "city_n": "8"},
        "M": {"city_c": "南投縣", "city_n": "11"},
        "N": {"city_c": "彰化縣", "city_n": "12"},
        "O": {"city_c": "新竹市", "city_n": "6"},
        "P": {"city_c": "雲林縣", "city_n": "13"},
        "Q": {"city_c": "嘉義縣", "city_n": "15"},
        "T": {"city_c": "屏東縣", "city_n": "20"},
        "U": {"city_c": "花蓮縣", "city_n": "22"},
        "V": {"city_c": "臺東縣", "city_n": "21"},
        "X": {"city_c": "澎湖縣", "city_n": "20"},
        "W": {"city_c": "金門縣", "city_n": "24"},
        "Z": {"city_c": "連江縣", "city_n": "25"}
        }
    
    def get_land_items(self, city_code):
        city_n = self.all_city[city_code]['city_n']
        city = self.all_city[city_code]['city_c']
        list_obj = []
        page, last_page = 0, 1
        headers = {'User-Agent': self.user_agent.random}
        while page<last_page:
            page+=1
            url = 'http://www.chumken.com.tw/Function/Ajax/SearchObj.aspx'
            data = f'func=AllObj&HotObj=0&TabID=Obj_List&RS_Type=2&CityID={city_n}&AreaID_List=&ObjType=I.土地&Pg1_ID=Pg1&Pg1={page}&OrderS_ID=OrderS&OrderS='
            try:
                req = urllib.request.Request(url, data.encode("utf-8"), headers)
                with urllib.request.urlopen(req) as f:
                    res = f.read()
                html = res.decode()
                if '很抱歉！查無相關資料！' in html: # 0筆資料的
                    break
                soup = BeautifulSoup(html, 'lxml')
                items = soup.find_all('a')
                for item in items:
                    if 'sell_item' in item['href']:
                        link = 'http://www.chumken.com.tw' + item['href']
                        subject = item.find('div',{'class':'title-h1'}).text
                        source_id = re.findall('sell_item/(\w+-\w+)/', link)[0]
                        price = item.find('div',{'class':'text-p'}).find_all('p')[1].text
                        re_price = int(re.findall(r'總價：(\d+)\.?\d*萬', price)[0])
                        obj_data = {
                                    "source_id" : source_id,
                                    "subject" : subject,
                                    "link" : link,
                                    "city" : city,
                                    "re_price" : int(re_price),
                                    "insert_time" : time.strftime("%Y-%m-%d %H:%M:%S")
                                    }
                        list_obj.append(obj_data)
                    else:
                        last_page = int(re.findall("value='(\d+)'", item['onclick'])[0]) # 有一頁以上資料的

            except Exception as e:
                self.logger.error(f"Error processing Land City {city_code} Page {page}: {str(e)}")

        else:
            print(f'City {city_code} 共{len(list_obj)}個物件')
        
        if list_obj:
            to_sqlite(self, list_obj)
        else: # 是空的也要建立空資料表
            con = sqlite3.connect(os.path.abspath('compare.db'))
            table_name = f'{self.name}_{time.strftime("%m%d")}'
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} (source_id, subject, link, city, re_price, insert_time)"
            con.execute(sql)
            con.close()
        return list_obj

    def get_hosue_items(self, city_code):
        city_n = self.all_city[city_code]['city_n']
        city = self.all_city[city_code]['city_c']
        list_obj = []
        page, last_page = 0, 1
        headers = {'User-Agent': self.user_agent.random}
        while page<last_page:
            page+=1
            url = 'http://www.chumken.com.tw/Function/Ajax/SearchObj.aspx'
            data = f'func=AllObj&HotObj=0&TabID=Obj_List&RS_Type=2&CityID={city_n}&AreaID_List=&Pg1_ID=Pg1&Pg1={page}&OrderS_ID=OrderS&OrderS='
            try:
                time.sleep(random.uniform(1,2))
                req = urllib.request.Request(url, data.encode("utf-8"), headers)
                with urllib.request.urlopen(req) as f:
                    res = f.read()
                html = res.decode()
                
                if '很抱歉！查無相關資料！' in html:
                    break
                
                soup = BeautifulSoup(html, 'lxml')
                items = soup.find_all('a')
                for item in items:
                    if 'sell_item' in item['href']:
                        link = 'http://www.chumken.com.tw' + item['href']
                        subject = item.find('div',{'class':'title-h1'}).text
                        source_id = re.findall('sell_item/(\w+-\w+)/', link)[0]
                        price = item.find('div',{'class':'text-p'}).find_all('p')[1].text
                        re_price = int(re.findall(r'總價：(\d+)\.?\d*萬', price)[0])
                        obj_data = {
                                    "source_id" : source_id,
                                    "subject" : subject,
                                    "link" : link,
                                    "city" : city,
                                    "re_price" : int(re_price),
                                    "insert_time" : time.strftime("%Y-%m-%d %H:%M:%S")
                                    }
                        list_obj.append(obj_data)
                    else:
                        last_page = int(re.findall("value='(\d+)'", item['onclick'])[0])
            
            except Exception as e:
                self.logger.error(f"Error processing House City {city_code} Page {page}: {str(e)}")
        
        if list_obj:
            to_sqlite(self, list_obj)
        else: # 是空的也要建立空資料表
            con = sqlite3.connect(os.path.abspath('compare.db'))
            table_name = f'{self.name}_{time.strftime("%m%d")}'
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} (source_id, subject, link, city, re_price, insert_time)"
            con.execute(sql)
            con.close()
        return list_obj


    def get_data(self, city_code):
        list_obj = is_new(self, city_code)
        headers =  {'User-Agent': self.user_agent.random}
        proxy = get_proxies()
        data_list = []
        for obj in list_obj:
            subject = obj['subject']
            link = obj['link']
            source_id = obj['source_id']
            total = obj['re_price']
            city = obj['city']
            print(subject, link)
            try:
                time.sleep(random.uniform(1,2))
                res = requests.get(link, headers=headers, proxies=proxy)
                soup = BeautifulSoup(res.text, 'lxml')
                house_age, house_age_v = '', 0
                total_ping, building_ping, att_ping, public_ping, land_ping, total_floor = 0,0,0,0,0,0
                blockto, parking_type, house_type, situation, pattern, feature, manage_fee, community = '', '', '', '', '', '', '', ''
                contact_man, brand, contact_type = '', '', ''
                infos = soup.find_all('div',{'class':'object-title-p'})
                for info in infos:
                    match = re.findall('位　置：(.+)', info.text)
                    if match:
                        address = match[0].replace('台','臺')
                    match = re.findall('屋　齡：(.+)', info.text)
                    if match:
                        house_age = match[0]
                        match1 = re.findall('(\d+\.*\d*)', house_age)
                        if match1:
                            house_age_v = float(match1[0])
                    match = re.findall('朝　向：(.+)', info.text)
                    if match:
                        blockto = match[0]
                    match = re.findall('車位/備註：(.+)', info.text)
                    if match:
                        parking_type = match[0]
                    match = re.findall('總樓高：(\d+)', info.text)
                    if match:
                        total_floor = int(match[0])
                    match = re.findall('管理費：(.+)', info.text)
                    if match:
                        manage_fee = match[0]
                    match = re.findall('型　態：(.+)', info.text)
                    if match:
                        house_type = match[0]
                    match = re.findall('格　局：(.+)', info.text)
                    if match:
                        pattern = match[0]
                    match = re.findall('類　別：(.+)', info.text)
                    if match:
                        situation = match[0]
                    match = re.findall('物件特色：(.+)', info.text)
                    if match:
                        feature = match[0]
                    match = re.findall('權狀坪數[^：]*：(\d+\.*\d*) 坪', info.text)
                    if match:
                        total_ping = float(match[0])
                    match = re.findall('主建物：(\d+\.*\d*) 坪', info.text)
                    if match:
                        building_ping = float(match[0])
                    match = re.findall('附屬建物：(\d+\.*\d*) 坪', info.text)
                    if match:
                        att_ping = float(match[0])
                    match = re.findall('公共設施：(\d+\.*\d*) 坪', info.text)
                    if match:
                        public_ping = float(match[0])
                    match = re.findall('地　坪：(\d+\.*\d*) 坪', info.text)
                    if match:
                        land_ping = float(match[0])
                    match = re.findall('姓名：(.+)', info.text)
                    if match:
                        contact_man = match[0]
                        contact_type = '仲介'
                    match = re.findall('經紀業名稱：(.+)', info.text)
                    if match:
                        brand = match[0]
                area, road = '', ''
                locate = soup.find('meta',{'id':'og_description'})['content']
                match = re.search(f'(.+?(市區|鎮區|鎮市|[鄉鎮市區]))', address)
                if match:
                    area = match.group(1)
                    match = re.search(f'{area}(.+)', address)
                    if match:
                        road = match.group(1)
                match = re.findall('社區：(.+)', locate)
                if match:
                    community = match[0]
                branch, phone, img_url = '', '', ''
                shop = soup.find('div',{'class':'em10'})
                if shop:
                    match = re.findall('店　名：(.+) /', shop.text)
                    if match:
                        branch = match[0]
                    match = re.findall('電話：(.+)', shop.text)
                    if match:
                        phone = match[0]
                img_block = soup.find('div',{'class':'container'})
                if img_block:
                    img_url = img_block.find('img')['src']
                    if 'no-picture.jpg' in img_url:
                        img_url = ''
                if total_ping!= 0:
                    price_ave = round(total/total_ping,2)
                else:
                    if land_ping!=0:
                        price_ave = round(total/land_ping,2)
                    else:
                        price_ave = 0
            
            except requests.exceptions.ProxyError as err:
                self.logger.error(f"Proxy Error processing link {link}: {str(err)}")
                time.sleep(random.uniform(5,10))
                proxy = get_proxies()
                continue

            except Exception as err:
                self.logger.error(f"Error processing link {link}: {str(err)}")
                continue
            # 此網站無提供以下資訊
            pattern1, house_num, edge, dark, floor_web, manage_type, company, lat, lng, floor = '', '', '', '', '', '', '', 0, 0, 0
            
            # 歷史資料
            his_data = {
                        "source" : self.source,
                        "source_id" : source_id,
                        "total" : str(total),
                        "subject" : subject,
                        "insert_time" : time.strftime("%Y-%m-%d"),
                        "link" : link
                        }
            history = f'[{his_data}]'.replace("'", '"')

            data = dict()
            data["source"]=self.source
            data["source_id"]=source_id
            data["subject"]=subject
            data["city"]=city
            data["area"]=area
            data["road"]=road
            data["address"]=address
            data["situation"]=situation
            data["total"]=total
            data["price_ave"]=price_ave
            data["feature"]=feature
            data["pattern"]=pattern
            data["pattern1"]=pattern1
            data["total_ping"]=total_ping
            data["building_ping"]=building_ping
            data["public_ping"]=public_ping
            data["att_ping"]=att_ping
            data["land_ping"]=land_ping
            data["house_age"]=house_age
            data["house_age_v"]=house_age_v
            data["floor_web"]=floor_web
            data["floor"]=floor
            data["total_floor"]=total_floor
            data["house_num"]=house_num
            data["blockto"]=blockto
            data["house_type"]=house_type
            data["manage_type"]=manage_type
            data["manage_fee"]=manage_fee
            data["edge"]=edge
            data["dark"]=dark
            data["parking_type"]=parking_type
            data["lat"]=lat
            data["lng"]=lng
            data["link"]=link
            data["img_url"]=img_url
            data["contact_type"]=contact_type
            data["contact_man"]=contact_man
            data["phone"]=phone
            data["brand"]=brand
            data["branch"]=branch
            data["company"]=company
            data["price_renew"]=0
            data["insert_time"]=time.strftime("%Y-%m-%d %H:%M:%S")
            data["update_time"]=time.strftime("%Y-%m-%d %H:%M:%S")
            data["community"]=community
            data["mrt"]=''
            data["group_man"]=0
            data["group_key"]=''
            data["group_record"]=''
            data["history"]=history
            data["address_cal"]=''
            data["is_delete"]=0
            data["is_hidden"]=0
            # print(data)
            data_list.append(clean_data(data))
            
            # 如果超過100筆物件，就先寫入資料庫，並清空data_list
            if len(data_list) % 100 == 0:
                data2sql(data_list, city_code)
                data_list = []

        data2sql(data_list, city_code)
        # 把物件都寫進資料庫之後，再寫入group_key跟address_cal
        find_group(self, city_code)
        find_possible_addrs(self, city_code)