import requests
from bs4 import BeautifulSoup
import re
import time
import pandas as pd
from fake_useragent import UserAgent
import random
from crawler.craw_common import *

class Etwarm():
    def __init__(self):
        self.name = 'etwarm'
        self.source = '東森房屋'
        self.user_agent = UserAgent()
        self.logger = WriteLogger().getLoggers()
        self.all_city_dict = {
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

    def get_items(self, city_code):
        city_n = self.all_city_dict[city_code].replace('臺','台')
        house_list = []
        url = f'https://www.etwarm.com.tw/houses?searchKind=buy&area={city_n}&sort=DEFAULT&page=1'
        header = {"User-Agent": self.user_agent.random}
        proxy = get_proxies()
        s = requests.Session()
        time.sleep(random.randint(1,5))
        r = s.get(url, headers=header, proxies=proxy)
        soup = BeautifulSoup(r.text, 'lxml')
        token_item = soup.select_one('meta[name="csrf-token"]')
        token = token_item.get('content')
        header['X-CSRF-TOKEN'] = token
        index_url = 'https://www.etwarm.com.tw/houses/buy-list-json'
        page, last_page = 0,1
        while page<last_page:
            page+=1
            data = { 'page': page, 'sort': "NEW", 'area': city_n }
            try:
                time.sleep(random.uniform(1,3))
                res = s.post(index_url, json=data, headers=header, proxies=proxy)
                page_list = res.json()['data']
                last_page = res.json()['last_page']
                if not page_list:
                    break
                for obj in page_list:
                    house = dict()
                    if obj['物件類別'] == '預售屋':
                        continue
                    house['isLand'] = obj['isLand']
                    house['source_id'] = obj['編號']
                    house['source'] = self.source
                    house['subject'] = obj['案名']
                    house['link'] = obj['物件詳細頁']
                    house['re_price'] = int(float(obj['刊登售價']))
                    house['img_url'] = obj['多媒體']['照片'][0]
                    house['地址'] = obj['地址']
                    house['city'] = all_city_dict[city_code]
                    house['鄉鎮市區'] = obj['鄉鎮市區']
                    house['路'] = obj['路']
                    house['土地坪數'] = obj['土地坪數']
                    house['座向'] = obj['座向']
                    house['物件類別'] = obj['物件類別']
                    house['特色推薦'] = obj['特色推薦']
                    house['總坪數'] = obj['總坪數']
                    # 建物坪數是含附屬建物的！！！
                    house['建物坪數'] = obj['建物坪數']
                    house['屋齡'] = obj['屋齡']
                    house['車位'] = obj['車位']
                    #有些沒格局這個key
                    try:
                        house['格局'] = house['show_item']['格局']
                    except:
                        house['格局'] = ''
                    house['insert_time'] = time.strftime("%Y-%m-%d %H:%M:%S")
                    house_list.append(house)
            
            except Exception as e:
                self.logger.error(f"Error processing City {city_code} Page {page}: {str(e)}")
            
        if house_list:
            to_sqlite(self, house_list)
        else: # 是空的也要建立空資料表
            con = sqlite3.connect(os.path.abspath('compare.db'))
            table_name = f'{self.name}_{time.strftime("%m%d")}'
            sql = f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
            "isLand"	INTEGER,
            "source_id"	TEXT,
            "source"	TEXT,
            "subject"	TEXT,
            "link"	TEXT,
            "re_price"	REAL,
            "img_url"	TEXT,
            "地址"	TEXT,
            "city"	TEXT,
            "鄉鎮市區"	TEXT,
            "路"	TEXT,
            "土地坪數"	TEXT,
            "座向"	TEXT,
            "物件類別"	TEXT,
            "特色推薦"	TEXT,
            "總坪數"	REAL,
            "建物坪數"	REAL,
            "屋齡"	TEXT,
            "車位"	TEXT,
            "格局"	TEXT,
            "insert_time"	TEXT
            );
            '''
            con.execute(sql)
            con.close()

    def get_data(self, city_code):
        house_list = is_new(self, city_code)
        data_list = []
        proxy = get_proxies()
        for house in house_list:
            header = {"User-Agent": self.user_agent.random}
            subject = house['subject']
            link = house['link']
            print(subject, link)
            source_id = house['source_id']
            address = house['地址'].replace('台','臺')
            city = house['city']
            area = house['鄉鎮市區']
            road = house['路']
            if '0' in road:
                road =''
            elif 'X' in road:
                road = ''
            total = house['re_price']
            land_ping = house['土地坪數']
            land_ping = float(land_ping) if land_ping else 0
            blockto = house['座向']
            if blockto == '暫未調查':
                blockto = ''
            house_type = house['物件類別']
            img_url = house['img_url']
            feature = house['特色推薦'].split('東森168')[0].strip()
            time.sleep(random.uniform(1,3))
            try:
                res = requests.get(link, headers=header, proxies=proxy)
                soup = BeautifulSoup(res.text, 'lxml')
                map = soup.select_one('#house-life-tab1')
                if map:
                    lat = round(float(map['data-lat']),7)
                    lng = round(float(map['data-lng']),7)
                else:
                    lat, lng = 0,0
                contact_type = '仲介'
                brand = '東森房屋'
                contact_man, branch, phone, company = '', '', '', ''
                is_man = soup.select_one('#app > main > section:nth-child(5) > div > div > div.col-md-r > div.broker-box > div > div.d-flex.flex-column > div:nth-child(1) > span')
                if is_man:
                    contact_man = is_man.text
                is_branch = soup.select_one('#app > main > section:nth-child(5) > div > div > div.col-md-r > div.broker-box > div > div.d-flex.flex-column > div:nth-child(1) > a > span')
                if is_branch:
                    branch = is_branch.text
                is_company = soup.select_one('#app > main > section:nth-child(5) > div > div > div.col-md-r > div.broker-box > div > div.d-flex.flex-column > div:nth-child(2)')
                if is_company:
                    company = is_company.text
                is_phone = soup.select_one('#app > main > section:nth-child(5) > div > div > div.col-md-r > div.broker-box > div > div.d-flex.flex-column > div.font-20.w-100 > a')
                if is_phone:
                    phone = is_phone.text
                infos = soup.find_all('div',{'class':'col-md-4 d-table'})
                for info in infos:
                    if info.find_all('div',{'class':'d-table-cell color-5 w-01'}):
                        if info.find_all('div',{'class':'d-table-cell color-5 w-01'})[0].text == '類型':
                            type = info.find_all('div',{'class':'d-table-cell'})[1].text
                            if '/' in type:
                                situation = type.split('/')[1].strip()
                            else:
                                situation = ''
                        elif info.find_all('div',{'class':'d-table-cell color-5 w-01'})[0].text == '管理費':
                            manage_fee = info.find_all('div',{'class':'d-table-cell'})[1].text
                        elif info.find_all('div',{'class':'d-table-cell color-5 w-01'})[0].text == '社區':
                            community = info.find_all('div',{'class':'d-table-cell'})[1].text
                total_ping, building_ping, public_ping = 0,0,0
                house_age, house_age_v, floor_web, floor, total_floor = '', 0, '', 0, 0
                pattern, parking_type = '', ''
                if not house['isLand']: # 若是土地，網站無提供以下資訊
                    total_ping = house['總坪數']
                    # 建物坪數是含附屬建物的！！！
                    building_ping = house['建物坪數']
                    house_age = house['屋齡']
                    if house_age:
                        house_age_v = re.findall('\d+\.*\d*', house_age)[0]
                    else:
                        house_age_v = 0
                    parking_type = house['車位']
                    pattern = house['格局']
                    is_public = soup.select_one('#app > main > section:nth-child(7) > div > div > div.row.w-100.font-15.object-data-box.align-items-center > div.col-md4.d-table > div:nth-child(2)')
                    if is_public:
                        if '－' not in is_public.text:
                            public_ping = re.findall(r'約(\d+\.*\d*)坪', is_public.text)[0]
                    for info in infos:
                        if info.find_all('div',{'class':'d-table-cell color-5 w-01'}):
                            if info.find_all('div',{'class':'d-table-cell color-5 w-01'})[0].text == '樓層':
                                floors = info.find_all('div',{'class':'d-table-cell'})[1].text
                                floor_web = floors.split('/')[0]
                                match = re.findall('\d+', floors)
                                if match:
                                    total_floor = int(match[-1])
                                if 'B' in floor_web:
                                    floor = -1
                                else:
                                    match = re.findall('\d+', floor_web)
                                    if match:
                                        floor = int(match[0])
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

            except Exception as e:
                self.logger.error(f"Error processing link {link}: {str(e)}")
                continue
            
            # 此網站無提供以下資訊
            pattern1, house_num, edge, dark, manage_type = '', '', '', '', ''
            att_ping = 0 #此網頁僅提供'主建物+附屬建物'坪數

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