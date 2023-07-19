import requests
from bs4 import BeautifulSoup
import pandas as pd
import os,re,time,json,math
from fake_useragent import UserAgent
import random
from crawler.craw_common import *

class House588():
    def __init__(self):
        self.name = 'house588'
        self.source = '588房訊網'
        self.user_agent = UserAgent()
        self.logger = WriteLogger().getLoggers()
        self.all_city_dict = {
        "A": {"city_c": "臺北市", "city_n": "2"},
        "B": {"city_c": "臺中市", "city_n": "8"},
        "C": {"city_c": "基隆市", "city_n": "1"},
        "D": {"city_c": "臺南市", "city_n": "14"},
        "E": {"city_c": "高雄市", "city_n": "15"},
        "F": {"city_c": "新北市", "city_n": "3"},
        "G": {"city_c": "宜蘭縣", "city_n": "19"},
        "H": {"city_c": "桃園市", "city_n": "4"},
        "I": {"city_c": "嘉義市", "city_n": "12"},
        "J": {"city_c": "新竹縣", "city_n": "6"},
        "K": {"city_c": "苗栗縣", "city_n": "7"},
        "M": {"city_c": "南投縣", "city_n": "10"},
        "N": {"city_c": "彰化縣", "city_n": "9"},
        "O": {"city_c": "新竹市", "city_n": "5"},
        "P": {"city_c": "雲林縣", "city_n": "11"},
        "Q": {"city_c": "嘉義縣", "city_n": "13"},
        "T": {"city_c": "屏東縣", "city_n": "16"},
        "U": {"city_c": "花蓮縣", "city_n": "18"},
        "V": {"city_c": "臺東縣", "city_n": "17"},
        "X": {"city_c": "澎湖縣", "city_n": "20"},
        "W": {"city_c": "金門縣", "city_n": "21"},
        "Z": {"city_c": "連江縣", "city_n": "22"}
        }

    def get_items(self, city_code):
        city = self.all_city_dict[city_code]['city_c']
        city_n = self.all_city_dict[city_code]['city_n']
        house_list = []
        url = 'https://588house.com.tw/Search/Sale'
        header = {'user-agent':self.user_agent.random}
        proxy = get_proxies()
        s = requests.Session()
        r = s.get(url, headers=header, proxies=proxy)
        soup = BeautifulSoup(r.text, 'lxml')
        token= soup.select_one('#commonForm > input[type=hidden]')['value']
        url = f'https://588house.com.tw/Search/AjaxSearchS'
        page, last_page = 0,1
        while page<last_page:
            page+=1
            data = {
                'city_ids': city_n,
                'page': page,
                'mode': 1,
                'price_range': '100,30000',
                'sort': 1,
                '__RequestVerificationToken' : token
            }
            try:
                time.sleep(random.uniform(1,3))
                res = s.post(url, data=data, headers=header, proxies=proxy)
                house_data = json.loads(res.json()['msg'])
                last_page = house_data['page_count']
                page_list = house_data['items']
                print(f'Page{page}/{last_page} {len(page_list)}個物件')
                for house in page_list:
                    house['source_id'] = str(house['id'])
                    del house['id']
                    house['source'] = self.source
                    house['city'] = city
                    house['re_price'] = int(house.pop('price'))
                    house_list.append(house)
            except Exception as e:
                self.logger.error(f"Error processing City {city_code} Page {page}: {str(e)}")
        
        if house_list:
            to_sqlite(self, house_list)
        else: # 是空的也要建立空資料表
            con = sqlite3.connect(os.path.abspath('compare.db'))
            table_name = f'{self.name}_{time.strftime("%m%d")}'
            sql = f'''CREATE TABLE IF NOT EXISTS {table_name} (
                "code"	TEXT,
                "title"	TEXT,
                "purpose"	INTEGER,
                "type"	REAL,
                "floor"	INTEGER,
                "floor_count"	INTEGER,
                "full_addr"	TEXT,
                "lat"	REAL,
                "lng"	REAL,
                "community_id"	TEXT,
                "community_name"	TEXT,
                "area"	REAL,
                "room_text"	TEXT,
                "build_age"	INTEGER,
                "unit_price"	REAL,
                "contact_type"	INTEGER,
                "contact_company"	TEXT,
                "contact_name"	TEXT,
                "image_url"	TEXT,
                "view_count"	INTEGER,
                "favorite_count"	INTEGER,
                "is_favorite"	INTEGER,
                "update_time"	TEXT,
                "source_id"	TEXT,
                "source"	TEXT,
                "city"	TEXT,
                "re_price"	INTEGER
            );
            '''
            con.execute(sql)
            con.close()

    def get_data(self, city_code):
        house_list = is_new(self, city_code)
        header = {'user-agent':self.user_agent.random}
        proxy = get_proxies()
        for house in house_list:
            data_list = []
            subject = house['title']
            source_id = str(house['source_id'])
            link = 'https://588house.com.tw/Sale/' + source_id
            print(subject, link)
            address = house['full_addr'].replace('台','臺')
            city, area, road = '', '', ''
            match = re.search(f'(.+?[縣市])', address)
            if match:
                city = match.group(1)
            match = re.search(f'{city}?(.+?(市區|鎮區|鎮市|[鄉鎮市區]))', address)
            if match:
                area = match.group(1)
                match = re.search(f'{city}?{area}(.+)', address)
                if match:
                    road = match.group(1)
            else:
                match = re.search(f'{city}?(.+)', address)
                if match:
                    road = match.group(1)
            lat = house['lat']
            lng = house['lng']
            if not lat or math.isnan(lat):
                lat = 0
            if not lng or math.isnan(lng):
                lng = 0
            pattern = house['room_text']
            if not pattern :
                pattern = ''
            contact_man, brand, branch, company = '', '', '', ''
            contact_company = house['contact_company'] # 可能包含company跟brand或寫branch
            if contact_company:
                match = re.findall('[\u4e00-\u9fff]+[房屋|不動產]', contact_company)
                if match:
                    brand = match[0]
                match = re.findall('[\u4e00-\u9fff]+[公司|企業]', contact_company)
                if match:
                    company = match[0]
                match = re.findall('[\u4e00-\u9fff]+[加盟店|直營店|店]', contact_company)
                if match:
                    branch = match[0]
            contact_name = house['contact_name'] # 可能寫man或branch
            if contact_name:
                match = re.findall('[\u4e00-\u9fff]+[加盟店|直營店|店]', contact_name)
                if match:
                    branch = match[0]
                else:  # 若不符合店名branch格式，歸類至contact_man
                    contact_man = contact_name
            img_url = house['image_url']
            # update_time = house['update_time']
            total = house['re_price']
            price_ave = house['unit_price']
            total_ping = house['area']
            house_age_v = house['build_age']
            house_age = str(house_age_v) + '年'
            community = house['community_name']
            if not community:
                community = ''
            floor = house['floor'] # 整棟或無資料都會寫-99
            if floor == '-99':
                floor_web, floor = '', 0
            else:
                floor_web = str(floor)
            total_floor = house['floor_count']
            # 以下資訊需進入各物件頁面中擷取
            try:
                time.sleep(random.uniform(2,4))
                res = requests.get(link, headers=header, proxies=proxy)
                soup = BeautifulSoup(res.text, 'lxml')
                is_feature = soup.select_one('#Detail > div:nth-child(2) > p')
                feature = is_feature.text.strip() if is_feature else ''
                house_type = ''
                if house['type']:
                    if not math.isnan(house['type']):
                        type_id = str(int(house['type']))
                        house_type = soup.select_one('#typesSel').find('option',{'value':type_id}).text
                purpose_id = str(house['purpose'])
                if purpose_id != 'None':
                    situation = soup.select_one('#purposesSel').find('option',{'value':str(purpose_id)}).text
                else:
                    situation = ''
                contact = str(house['contact_type'])
                if contact == '0':
                    contact_type = '屋主'
                elif contact == '2':
                    contact_type = '仲介'
                else:
                    contact_type = ''
                manage_fee, blockto, parking_type = '', '', ''
                building_ping, public_ping, att_ping, land_ping = 0,0,0,0
                detail = soup.select_one('#content').text
                match = re.findall('管理費用(.+)', detail)
                if match:
                    manage_fee = match[0]
                match = re.findall('朝向(.+)', detail)
                if match:
                    blockto = match[0]
                match = re.findall('車位類型(.+)', detail)
                if match:
                    parking_type = match[0]
                match = re.findall('主建物坪數(\d+\.?\d*)', detail)
                if match:
                    building_ping = float(match[0])
                match = re.findall('共有部分坪數(\d+\.?\d*)', detail)
                if match:
                    public_ping = float(match[0])
                match = re.findall('附屬建物坪數(\d+\.?\d*)', detail)
                if match:
                    att_ping = float(match[0])
                match = re.findall('土地坪數(\d+\.?\d*)', detail)
                if match:
                    land_ping = float(match[0])
                is_phone = soup.find('a',{'class':'tel'})
                if is_phone:
                    phone = is_phone.text
                else:
                    phone = ''
            
            except requests.exceptions.ProxyError as err:
                self.logger.error(f"Proxy Error processing link {link}: {str(err)}")
                time.sleep(random.uniform(5,10))
                proxy = get_proxies()
                continue

            except Exception as e:
                self.logger.error(f"Error processing link {link}: {str(e)}")
                continue

            # 此網站無提供以下資訊
            pattern1, house_num, manage_type, edge, dark = '', '', '', '', ''
            
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