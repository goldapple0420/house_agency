from dataclasses import dataclass
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re,time,json
import random
from fake_useragent import UserAgent
import pandas as np
from crawler.craw_common import *
import unicodedata

class House591():
    def __init__(self):
        self.name = 'house591'
        self.source = '591'
        self.user_agent = UserAgent()
        self.logger = WriteLogger().getLoggers()
        self.all_city_dict = {
            "A": {"city_c": "臺北市", "city_n": "1"},
            "B": {"city_c": "臺中市", "city_n": "8"},
            "C": {"city_c": "基隆市", "city_n": "2"},
            "D": {"city_c": "臺南市", "city_n": "15"},
            "E": {"city_c": "高雄市", "city_n": "17"},
            "F": {"city_c": "新北市", "city_n": "3"},
            "G": {"city_c": "宜蘭縣", "city_n": "21"},
            "H": {"city_c": "桃園市", "city_n": "6"},
            "I": {"city_c": "嘉義市", "city_n": "12"},
            "J": {"city_c": "新竹縣", "city_n": "5"},
            "K": {"city_c": "苗栗縣", "city_n": "7"},
            "M": {"city_c": "南投縣", "city_n": "11"},
            "N": {"city_c": "彰化縣", "city_n": "10"},
            "O": {"city_c": "新竹市", "city_n": "4"},
            "P": {"city_c": "雲林縣", "city_n": "14"},
            "Q": {"city_c": "嘉義縣", "city_n": "13"},
            "T": {"city_c": "屏東縣", "city_n": "19"},
            "U": {"city_c": "花蓮縣", "city_n": "23"},
            "V": {"city_c": "臺東縣", "city_n": "22"},
            "X": {"city_c": "澎湖縣", "city_n": "24"},
            "W": {"city_c": "金門縣", "city_n": "25"},
            "Z": {"city_c": "連江縣", "city_n": "26"}
            }

    # 中古屋house 跟 店面住辦辦公廠房土地business＆land 列表回傳的dict完全不一樣，所以放不同資料表裡
    def get_house_items(self, city_code):
        city_n = self.all_city_dict[city_code]['city_n']
        city = self.all_city_dict[city_code]['city_c']
        # 先取得token
        url = 'https://sale.591.com.tw/'
        header = {"User-Agent": self.user_agent.random}
        proxy = get_proxies()
        s = requests.Session()
        time.sleep(random.randint(1,5))
        r = s.get(url, headers=header, proxies=proxy)
        soup = BeautifulSoup(r.text, 'lxml')
        token_item = soup.select_one('meta[name="csrf-token"]')
        token = token_item.get('content')
        header['X-CSRF-TOKEN'] = token
        # 取得每一頁的物件基本資料
        index_url = 'https://sale.591.com.tw/home/search/list'
        list_obj = []
        obj_count, total_count = 0,1
        page = 0
        while obj_count<total_count:
            page+=1
            param = {
                "type": "2",
                "regionid": city_n,
                "shType": "list",
                # 每個region第一頁的firstRow=0，第二頁firstRow=30，以此類推，每多一頁+30
                "firstRow": str((page-1)*30),
                "order": "posttime_desc" # 依刊登時間排序
                }
            # 在 cookie 設定地區縣市，避免某些條件無法取得資料
            s.cookies.set('urlJumpIp', city_n, domain='.591.com.tw')
            try:
                res = s.get(index_url, params=param, headers=header, proxies=proxy)
                page_list = res.json()['data']['house_list']
                print(f'開始擷取{self.source} City:{city_code} 房屋 P{page} 物件')
                obj_count += len(page_list)
                total_count = int(res.json()['data']['total'])
                for house in page_list:
                    if house['type'] == "2": # 預售屋(is_newhouse)資料不進來（前面有篩過了但還是會魚目混珠..）
                        house['subject'] = house.pop('title')
                        house['source_id'] = str(house.pop('houseid'))
                        house['link'] = 'https://sale.591.com.tw/home/house/detail/2/' + house['source_id'] + '.html'
                        house['re_price'] = int(house.pop('price'))
                        house['city'] = city
                        del house['tag']
                        list_obj.append(house)
            
            except requests.exceptions.ProxyError as e:
                self.logger.error(f"ProxyError processing House City{city_code} Page{page}: {str(e)}") 
                proxy = get_proxies()
            
            except Exception as e:
                self.logger.error(f"Error processing House City{city_code} Page{page}: {str(e)}")
        
        if list_obj:
            to_sqlite(self, list_obj)
        else: # 是空的也要建立空資料表
            con = sqlite3.connect(os.path.abspath('compare.db'))
            table_name = f'{self.name}_{time.strftime("%m%d")}'
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} (source_id, subject, link, city, re_price, insert_time)"
            con.execute(sql)
            con.close()

    def get_other_items(self, city_code):
        # kind的種類 店面出售5 辦公出售6 住辦出售12 廠房出售7 土地出售11
        kind_type = [5,6,7,12,11]
        city_n = self.all_city_dict[city_code]['city_n']
        city = self.all_city_dict[city_code]['city_c']
        list_obj = []
        for kind in kind_type:
            # 先取得token
            url = f'https://business.591.com.tw/?type=2&kind={kind}'
            header = {"User-Agent": self.user_agent.random}
            proxy = get_proxies()
            s = requests.Session()
            time.sleep(random.uniform(3,11))
            for _ in range(3):
                try:
                    r = s.get(url, headers=header, proxies=proxy)
                    soup = BeautifulSoup(r.text, 'lxml')
                    token_item = soup.select_one('meta[name="csrf-token"]')
                    token = token_item.get('content')
                    header['X-CSRF-TOKEN'] = token
                    break
                except:
                    time.sleep(random.uniform(15,30))
                    header = {"User-Agent": self.user_agent.random}
                    proxy = get_proxies()
            # 取得每頁的物件基本資訊
            index_url = 'https://business.591.com.tw/home/search/rsList'
            obj_count, total_count = 0,1
            page = 0
            while obj_count<total_count:
                page+=1
                param = {
                    "type": '2',
                    "kind": str(kind),
                    "regionid": city_n,
                    # 每個region第一頁的firstRow=0，第二頁firstRow=30，以此類推，每多一頁+30
                    "firstRow": str((page-1)*30),
                    }
                time.sleep(random.uniform(2,8))
                for _ in range(5):
                    try:
                        res = s.get(index_url, params=param, headers=header, proxies=proxy)
                        res.raise_for_status()
                        break
                    except:
                        proxy = get_proxies()
                        header['User-Agent'] = self.user_agent.random
                page_list = res.json()['data']['data']
                print(f'開始擷取{self.source} City:{city_code} 其他 P{page} 物件')
                obj_count += len(page_list)
                total_count = int(str(res.json()['records']).replace(',',''))
                for house in page_list:
                    house['source_id'] = str(house.pop('houseid'))
                    house['subject'] = house.pop('address_img_title')
                    house['link'] = 'https://sale.591.com.tw/home/house/detail/2/' + house['source_id'] + '.html'
                    house['re_price'] = int(house.pop('price').replace(',',''))
                    house['city'] = city
                    del house['surrounding']
                    del house['photoList']
                    del house['rentTag']
                    list_obj.append(house)

        if list_obj:
            self.name = 'other591'
            to_sqlite(self, list_obj)
        else: # 是空的也要建立空資料表
            con = sqlite3.connect(os.path.abspath('compare.db'))
            table_name = f'{self.name}_{time.strftime("%m%d")}'
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} (source_id, subject, link, city, re_price, insert_time)"
            con.execute(sql)
            con.close()


    def get_house_data(self, city_code):
        house_list = is_new(self, city_code)
        data_list = []
        for i in range(len(house_list)):
            source_id = house_list[i]['source_id']
            subject = house_list[i]['subject']
            link = house_list[i]['link']
            print(subject, link)
            city = house_list[i]['city']
            total = house_list[i]['re_price']
            if i % 15 == 0: # 每15個換一個IP
                proxy = get_proxies()
            header = {'User-Agent': self.user_agent.random }
            time.sleep(random.randint(1,5))
            for _ in range(3):
                try:
                    res = requests.get(link, headers=header, proxies=proxy)
                    res.raise_for_status()
                    break
                except:
                    # print('物件連接失敗，重新連線中，請等候15~30秒')
                    time.sleep(random.randint(15,30))
            if '物件找不到了' in res.text:
                # print('此物件已不存在')
                continue
            area = house_list[i]['section_name']
            road = house_list[i]['address']
            address = city + area + road
            pattern = house_list[i]['room']
            nick_name = house_list[i]['nick_name']
            if '仲介' in nick_name:
                contact_type = '仲介'
            elif '屋主' in nick_name:
                contact_type = '屋主'
            elif '代理人' in nick_name:
                contact_type = '代理人'
            else:
                # print(nick_name)
                contact_type = ''
            house_type = house_list[i]['shape_name']
            img_url = house_list[i]['photo_url']
            price_ave = float(house_list[i]['unitprice'])
            house_age = house_list[i]['houseage']
            house_age_v = house_age
            floor_web, floor, total_floor = '', 0, 0
            floors = house_list[i]['floor']
            if floors:
                floor_web, total_floor = floors.split('/')
                match = re.findall('\d+',total_floor)
                if match:
                    total_floor = int(match[0])
                else:
                    total_floor = 0
                if 'B' in floor_web:
                    floor = -1
                else:
                    match = re.findall('\d+',floor_web)
                    if match:
                        floor = int(match[0])
            total_ping = house_list[i]['area']
            building_ping = house_list[i]['mainarea']
            blockto, parking_type, community, situation, manage_fee = '', '', '', '', ''
            att_ping, public_ping, land_ping = 0,0,0
            feature, lat, lng = '', 0,0
            contact_man, brand, branch, phone, company = '', '', '', '', ''
            try:
                soup = BeautifulSoup(res.text, 'lxml')
                info_box_addr = soup.find_all('div',{'class':'info-addr-content'})
                for box in info_box_addr:
                    if box.find('span',{'class':'info-addr-key'}):
                        if box.find('span',{'class':'info-addr-key'}).text == '朝向':
                            blockto = box.find('span',{'class':'info-addr-value'}).text
                        elif box.find('span',{'class':'info-addr-key'}).text == '社區':
                            community = box.find('span',{'class':'info-addr-value'}).text
                details = soup.find_all('div',{'class':'detail-house-item'})
                for detail in details:
                    if detail.find('div',{'class':'detail-house-key'}):
                        if detail.find('div',{'class':'detail-house-key'}).text == '車位':
                            parking_type = detail.find('div',{'class':'detail-house-value'}).text
                        elif detail.find('div',{'class':'detail-house-key'}).text == '附屬建物':
                            att_ping = float(detail.find('div',{'class':'detail-house-value'}).text.rstrip("坪"))
                        elif detail.find('div',{'class':'detail-house-key'}).text == '共用部分':
                            public_ping = float(detail.find('div',{'class':'detail-house-value'}).text.rstrip("坪"))
                        elif detail.find('div',{'class':'detail-house-key'}).text == '土地坪數':
                            land_ping = float(detail.find('div',{'class':'detail-house-value'}).text.rstrip("坪"))
                        elif detail.find('div',{'class':'detail-house-key'}).text == '法定用途':
                            situation = detail.find('div',{'class':'detail-house-value'}).text
                        elif detail.find('div',{'class':'detail-house-key'}).text == '管理費':
                            manage_fee = detail.find('div',{'class':'detail-house-value'}).text
                is_feature = soup.select_one('#detail-feature-text')
                if is_feature:
                    feature = is_feature.get_text(separator=' ',strip=True)
                is_map = soup.select_one('#detail-map-free')
                if is_map:
                    map_url = is_map['src']
                    match = re.search('q=(\d+\.\d+),(\d+\.\d+)', map_url)
                    if match:
                        lat = round(float(match.group(1)),7)
                        lng = round(float(match.group(2)),7)
                is_man = soup.find('span',{'class':'info-span-name'})
                if is_man:
                    contact_man = is_man.text
                is_phone = soup.find('span',{'class':'info-host-word'})
                if is_phone:
                    phone = is_phone.text
                is_agent = soup.find('div',{'class':'info-detail-show'})
                if is_agent:
                    match = re.findall('經紀業：(.+)', is_agent.text)
                    if match:
                        company = match[0]
                    match = re.findall('公司名：(.+)', is_agent.text)
                    if match:
                        brand = match[0]
                    match = re.findall('分公司：(.+)', is_agent.text)
                    if match:
                        branch = match[0]
                subject = unicodedata.normalize('NFKD', subject)
                feature = unicodedata.normalize('NFKD', feature)

            except requests.exceptions.ProxyError as err:
                self.logger.error(f"Proxy Error processing link {link}: {str(err)}")
                time.sleep(random.uniform(15,30))
                proxy = get_proxies()
                continue

            except Exception as e:
                self.logger.error(f"Error processing link {link}: {str(e)}")
                continue
            
            # 此網站無提供以下資訊
            pattern1, house_num, edge, dark, manage_type = '', '', '', '', ''
            
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


    # 店面5、辦公6為一種格式 ; 住辦12、廠房7、土地11一種格式
    def get_other_data(self, city_code):
        self.name = 'other591'
        house_list = is_new(self, city_code)
        data_list = []
        for i in range(len(house_list)):
            source_id = house_list[i]['source_id']
            subject = house_list[i]['subject']
            link = house_list[i]['link']
            print(subject, link)
            city = house_list[i]['city']
            total = house_list[i]['re_price']
            
            # 有些物件可能無提供以下資訊
            pattern, pattern1, house_num, edge, dark, manage_type, house_type = '', '', '', '', '', '', ''
            total_ping, land_ping, building_ping, att_ping, public_ping = 0,0,0,0,0
            house_age, house_age_v, floor_web, floor, total_floor = '',0,'',0,0
            blockto, parking_type, community, manage_fee, situation = '', '', '', '', ''
            company, brand, branch, phone = '', '', '', ''
            lat, lng, feature = '', '', ''
            
            kind = house_list[i]['kind']
            area = house_list[i]['section_name']
            road = house_list[i]['street_name']
            address = city + area + road
            contact_type = house_list[i]['role_name']
            contact_man = house_list[i]['linkman']
            img_url = house_list[i]['filename']
            total_floor = house_list[i]['allfloor']
            floor = house_list[i]['floor'] # 整棟的話會寫99
            floor_web = house_list[i]['floorStr']
            house_age = house_list[i]['houseage']
            house_age_v = house_age
            total_ping = house_list[i]['area']
            building_ping = house_list[i]['mainarea']
            land_ping = house_list[i]['groundarea']
            att_ping = house_list[i]['balcony_area']
            price_ave = round((total / total_ping),2)
            if 'noImg' in img_url: # 無圖片會放這張
                img_url = ''
            
            # 以下資訊需進入到各物件頁面擷取
            header = {
                    'User-Agent': self.user_agent.random ,
                    'Referer':f'https://business.591.com.tw/?type=2&kind={kind}',
                    'Cookie':'T591_TOKEN=3pi1clpuaekfl1ork5u48cgaki'
                    }
            if i % 15 == 0: # 跑15個換一個IP
                proxy = get_proxies()
            time.sleep(random.uniform(5,15))
            for _ in range(5):
                try:
                    res = requests.get(link, headers=header, proxies=proxy, timeout=10)
                    res.raise_for_status()
                    break
                except:
                    # print('物件連接失敗，重新連線中，請等候15~30秒')
                    time.sleep(random.randint(15,30))
                    proxy = get_proxies()
                    user_agent=UserAgent()
                    header['User-Agent'] = user_agent.random
            if '物件找不到了' in res.text:
                # print('此物件已不存在')
                continue
            
            try:
                soup = BeautifulSoup(res.text, 'lxml')
                # 2023.05.30 店面、辦公新版html擷取資料
                if kind in (5,6):
                    map = soup.find('div',{'class':'map-info-container'})
                    if map:
                        match = re.search('q=(\d+\.\d+),(\d+\.\d+)',map['href'])
                        if match:
                            lat = match.group(1)
                            lng = match.group(2)
                    is_feature = soup.find('div',{'class':'article'})
                    if is_feature:
                        feature = is_feature.text.strip()
                    info = soup.find('section',{'class':'base-info-container'})
                    if info:
                        match = re.findall('共有部份(\d+\.?\d*)坪', info.text)
                        if match:
                            public_ping = float(match[0])
                        infos = info.find_all('div',{'class':'label-item'})
                        for info in infos:
                            match = re.findall('型態(.+)', info.text)
                            if match:
                                house_type = match[0]
                            match = re.findall('用途(.+)', info.text)
                            if match:
                                situation = match[0]
                            match = re.findall('管理費(.+)', info.text)
                            if match:
                                manage_fee = match[0]
                            match = re.findall('車位(.+)', info.text)
                            if match:
                                parking_type = match[0]
                            match = re.findall('朝向(.+)', info.text)
                            if match:
                                blockto = match[0]
                    is_phone = soup.select_one('#__nuxt > div.container > div > div.main-con-right > section > div.phone > div > button > span:nth-child(2)')
                    if is_phone:
                        phone = is_phone.text.strip()
                    agent = soup.find('div',{'class':'contact-info'})
                    is_company = agent.find('p',{'class':'econ-text'})
                    if is_company:
                        company = is_company.text
                    is_brand = agent.find('span',{'class':'econ-name'})
                    if is_brand:
                        brand = is_brand.text
                    script = soup.find('script')
                    if script:
                        match = re.findall('subcompanyname:"(.+)",', script.text)
                        if match:
                            branch = match[0]
                # 住辦12、廠房7、土地11
                else:
                    is_feature = soup.select_one('#detail-feature-text')
                    if is_feature:
                        feature = is_feature.text.strip()
                    is_phone = soup.find('span',{'class':'info-host-word'})
                    if is_phone:
                        phone = is_phone.text.strip()
                    is_agent = soup.find('div',{'class':'info-host-detail'})
                    if is_agent:
                        match = re.findall('經紀業：(.+)',is_agent.text)
                        if match:
                            company = match[0]
                        match = re.findall('公司名：(.+)',is_agent.text)
                        if match:
                            brand = match[0]
                        match = re.findall('分公司：(.+)',is_agent.text)
                        if match:
                            branch = match[0]
                    map = soup.select_one('#detail-map-free')
                    if map:
                        match = re.search('q=(\d+\.\d+),(\d+\.\d+)',map['src'])
                        if match:
                            lat = match.group(1)
                            lng = match.group(2)
                    
                    #（側邊資訊＆下方房屋介紹稍有不同）
                    # 住辦格式
                    if kind == 12:
                        community = house_list[i]['cases_name']
                        pattern = house_list[i]['layout']
                        info_box_addr = soup.find_all('div',{'class':'info-addr-content'})
                        for box in info_box_addr:
                            if box.find('span',{'class':'info-addr-key'}):
                                if box.find('span',{'class':'info-addr-key'}).text == '朝向':
                                    blockto = box.find('span',{'class':'info-addr-value'}).text.replace(' ','')
                        details = soup.find_all('div',{'class':'detail-house-item'})
                        for detail in details:
                            if detail.find('div',{'class':'detail-house-key'}):
                                if detail.find('div',{'class':'detail-house-key'}).text == '車位':
                                    parking_type = detail.find('div',{'class':'detail-house-value'}).text
                                elif detail.find('div',{'class':'detail-house-key'}).text == '管理費':
                                    manage_fee = detail.find('div',{'class':'detail-house-value'}).text
                                elif detail.find('div',{'class':'detail-house-key'}).text == '型態':
                                    house_type = detail.find('div',{'class':'detail-house-value'}).text
                                elif detail.find('div',{'class':'detail-house-key'}).text == '法定用途':
                                    situation = detail.find('div',{'class':'detail-house-value'}).text
                    
                    # 廠房格式
                    elif kind == 7:
                        situation = house_list[i]['kind_name']
                        info_box_addr = soup.find_all('div',{'class':'info-addr-content'})
                        for box in info_box_addr:
                            if box.find('span',{'class':'info-addr-key'}):
                                if box.find('span',{'class':'info-addr-key'}).text == '型態':
                                    house_type = box.find('span',{'class':'info-addr-value'}).text
                        details = soup.find_all('div',{'class':'detail-house-item'})
                        for detail in details:
                            if detail.find('div',{'class':'detail-house-key'}):
                                if detail.find('div',{'class':'detail-house-key'}).text == '車位':
                                    parking_type = detail.find('div',{'class':'detail-house-value'}).text
                                elif detail.find('div',{'class':'detail-house-key'}).text == '管理費':
                                    manage_fee = detail.find('div',{'class':'detail-house-value'}).text
                    
                    # 土地格式
                    elif kind == 11:
                        house_type = house_list[i]['layout'].lstrip('類別：')
                subject = unicodedata.normalize('NFKD', subject)
                feature = unicodedata.normalize('NFKD', feature)

            except requests.exceptions.ProxyError as err:
                self.logger.error(f"Proxy Error processing link {link}: {str(err)}")
                time.sleep(random.uniform(15,30))
                proxy = get_proxies()
                continue

            except  Exception as e:
                self.logger.error(f"Error processing link {link}: {str(e)}")
                continue
            
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