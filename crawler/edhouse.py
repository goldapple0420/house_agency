import requests
from bs4 import BeautifulSoup
import pandas as pd
import re,time,json
from fake_useragent import UserAgent
import random
from crawler.craw_common import *

class Edhouse():
    def __init__(self):
        self.name = 'edhouse'
        self.source = '東龍不動產'
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
        list_obj = []
        city_n = self.all_city_dict[city_code.replace('臺','台')]
        city = self.all_city_dict[city_code]
        headers = {'user-agent': self.user_agent.random}
        proxy = get_proxies()
        page = 0
        while True:
            page+=1
            page_url = f'https://www.ed-house.com.tw/buy/objects?page={page}&q%5Bcities%5D={city_n}'
            try:
                time.sleep(random.uniform(1,2))
                res = requests.get(page_url, headers=headers, proxies=proxy)
                soup = BeautifulSoup(res.text, 'lxml')
                main_obj = soup.find('div',{'class':'main-obj-list'})
                items = main_obj.find_all('a',{'class':'item'})
                if not items:
                    print(f'City {city_code} 共{len(list_obj)}個物件')
                    break
                for item in items:
                    match = re.findall('/buy/objects/.+?',item['href'])
                    if match:
                        link = item['href']
                        source_id = source_id = re.search(r'buy/objects/(\d+)', link).group(1)
                        subject = item.find('div',{'class':'obj-title'}).text.strip()
                        re_price = int(float(item.find('div',{'class':'price'}).text.strip().replace(',','').replace('萬','')))
                        obj_data = {
                                    "source_id" : source_id,
                                    "subject" : subject,
                                    "link" : link,
                                    "city" : city,
                                    "re_price" : int(re_price),
                                    "insert_time" : time.strftime("%Y-%m-%d %H:%M:%S")
                                    }
                        list_obj.append(obj_data)
            
            except requests.exceptions.ProxyError as err:
                self.logger.error(f"Proxy Error processing Page {page}: {str(err)}")
                time.sleep(random.uniform(5,10))
                proxy = get_proxies()
            
            except Exception as e:
                self.logger.error(f"Error processing City {city_code} Page {page}: {str(e)}")
        
        if list_obj:
            to_sqlite(self, list_obj)
        else: # 是空的也要建立空資料表
            con = sqlite3.connect(os.path.abspath('compare.db'))
            table_name = f'{self.name}_{time.strftime("%m%d")}'
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} (source_id, subject, link, city, re_price, insert_time)"
            con.execute(sql)
            con.close()

    def get_data(self, city_code):
        list_obj = is_new(self, city_code)
        data_list = []
        proxy = get_proxies()
        for obj in list_obj:
            try:
                subject = obj['subject']
                link = obj['link']
                source_id = obj['source_id']
                total = obj['re_price']
                city = obj['city']
                print(subject, link)
                header = {'User-Agent': self.user_agent.random }
                time.sleep(random.randint(3,10))
                res = requests.get(link, headers=header, proxies=proxy)
                if '查無此物件' in res.text:
                    continue
                soup = BeautifulSoup(res.text, 'lxml')
                address = soup.select_one('.address').text.replace('台','臺')
                area, road = '', ''
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
                detail = soup.find('div',{'class':'section-content'})
                total_ping, building_ping, land_ping, att_ping, public_ping = 0,0,0,0,0
                situation, house_type = '', ''
                floor_web, floor, total_floor = '', 0,0
                house_age, house_age_v = '', 0
                pattern, blockto, parking_type, manage_fee = '', '', '', ''
                if detail:
                    match = re.findall('建物總面積：(.+?)坪', detail.text)
                    if match:
                        total_ping = float(match[0])
                    match = re.findall('主建物：(.+?)坪地', detail.text)
                    if match:
                        building_ping = float(match[0])
                    match = re.findall('坪地坪：(.+?)坪附', detail.text)
                    if match:
                        land_ping = float(match[0])
                    match = re.findall('附屬：(.+?)坪公', detail.text)
                    if match:
                        att_ping = float(match[0])
                    match = re.findall('公設：(.+?)坪現', detail.text)
                    if match:
                        public_ping = float(match[0])
                    match = re.findall('現況用途：(.+?)物件', detail.text)
                    if match:
                        situation = match[0].strip()
                    match = re.findall('物件種類：(.+?)法定', detail.text)
                    if match:
                        house_type = match[0].strip()
                    else:
                        match = re.findall('物件種類：(.+?)樓層', detail.text)
                        if match:
                            house_type = match[0].strip()
                    match = re.findall('樓層：(.+?)屋齡', detail.text)
                    if match:
                        floor_s = match[0].strip()
                        if floor_s:
                            floor_web = floor_s.split('/')[0]
                            total_floor = floor_s.split('/')[1]
                            floor_web = floor_web.strip()
                            total_floor = total_floor.strip().rstrip('層')
                            floor = floor_web.rstrip('樓')
                            if '-' in floor:
                                floor = floor.split('-')[0]
                            if '地下' in floor_web:
                                floor = -1
                            try:
                                floor = int(floor)
                            except:
                                floor = 0
                    match = re.findall('屋齡：(.+?)格局', detail.text)
                    if match:
                        house_age = match[0].strip()
                        house_age_v = float(house_age.rstrip('年'))
                    if not house_age:
                        house_age_v = 0
                    match = re.findall('格局：(.+?)座向', detail.text)
                    if match:
                        pattern = match[0].strip()
                    match = re.findall('座向：(.+?)車位', detail.text)
                    if match:
                        blockto = match[0].strip()
                    if '管理費' in detail.text:
                        match = re.findall('車位：(.+?)管裡', detail.text)
                        if match:
                            parking_type = match[0].strip()
                        match = re.findall('管理費：(.+?)附', detail.text)
                        if match:
                            manage_fee = match[0].strip()
                    else:
                        match = re.findall('車位：(.+?)附近', detail.text)
                        if match:
                            parking_type = match[0].strip()
                    match = re.findall('每坪單價：(.+?)萬/坪', detail.text)
                    if match:
                        price_ave = float(match[0].strip())
                    else:
                        price_ave = 0
                if total==0:
                    if total_ping!= 0:
                        total = round(total_ping * price_ave, 1)
                    else:
                        if land_ping!=0:
                            total = round(land_ping * price_ave, 1)
                feature = ''
                for tag in soup.select('#main-obj-show > div.inbox > div.more-info > div:nth-child(2) > div > div.section-content > p'):
                    # 內容分行
                    contents = tag.get_text(separator="\n").splitlines()
                    # 跌代每一行
                    for line in contents:
                        # 各別去判斷每一行的情況
                        if tag.get_text() != "":
                            feature += line
                            feature = feature.strip()
                            feature += "\n"
                feature = feature.strip()
                photos = soup.find('div',{'class':'obj-photos-section'})
                img = photos.find('img')
                if img:
                    img_url = 'https://www.ed-house.com.tw/' + img.get_attribute_list('src')[0]
                else:
                    img_url = ''
                agent = soup.find('div',{'class':'main-agent-card'})
                if agent:
                    store = agent.find('div',{'class':'store-box'}).text
                    brand = store.split('-')[0]
                    branch = store.split('-')[-1]
                    contact_man = agent.find('span',{'class':'name'}).text
                    phone = agent.find('a',{'class':'phone'}).text
                map = soup.select_one('#main-obj-map')
                if map:
                    lat = map.get_attribute_list('data-lat')[0]
                    lng = map.get_attribute_list('data-lng')[0]
                else:
                    lat, lng = 0,0
            
            except Exception as err:
                self.logger.error(f"Error processing link {link}: {str(err)}")
                continue
            
            # 此網暫無提供以下資訊
            pattern1, house_num, manage_type, edge, dark, contact_type, company, community = '', '', '', '', '', '', '', ''
            
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