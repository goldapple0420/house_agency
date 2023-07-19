import requests
from bs4 import BeautifulSoup
import re
import time
import pandas as pd
import random
import urllib3
import certifi
import sqlite3, os
from fake_useragent import UserAgent
from crawler.craw_common import *
import sqlite3

requests.packages.urllib3.disable_warnings()
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL:@SECLEVEL=1'

class HouseWeb():
    def __init__(self):
        self.name = 'houseweb'
        self.source = '淘屋網'
        self.user_agent = UserAgent()
        self.logger = WriteLogger().getLoggers()
        self.all_city_dict = {
        "A": {"city_c": "臺北市", "city_n": "01"},
        "B": {"city_c": "臺中市", "city_n": "10"},
        "C": {"city_c": "基隆市", "city_n": "03"},
        "D": {"city_c": "臺南市", "city_n": "19"},
        "E": {"city_c": "高雄市", "city_n": "21"},
        "F": {"city_c": "新北市", "city_n": "02"},
        "G": {"city_c": "宜蘭縣", "city_n": "30"},
        "H": {"city_c": "桃園市", "city_n": "05"},
        "I": {"city_c": "嘉義市", "city_n": "17"},
        "J": {"city_c": "新竹縣", "city_n": "07"},
        "K": {"city_c": "苗栗縣", "city_n": "09"},
        "M": {"city_c": "南投縣", "city_n": "16"},
        "N": {"city_c": "彰化縣", "city_n": "13"},
        "O": {"city_c": "新竹市", "city_n": "06"},
        "P": {"city_c": "雲林縣", "city_n": "14"},
        "Q": {"city_c": "嘉義縣", "city_n": "18"},
        "T": {"city_c": "屏東縣", "city_n": "24"},
        "U": {"city_c": "花蓮縣", "city_n": "28"},
        "V": {"city_c": "臺東縣", "city_n": "26"},
        "X": {"city_c": "澎湖縣", "city_n": "23"},
        "W": {"city_c": "金門縣", "city_n": "32"}
        }

    def get_items(self, city_code):
        list_obj = []
        # 此網站無提供連江縣物件
        if city_code == 'Z':
            # print('該網站沒有提供連江縣物件')
            return list_obj
        
        city_n = self.all_city_dict[city_code]['city_n']
        city = self.all_city_dict[city_code]['city_c']
        url = 'https://www.houseweb.com.tw/sale'
        header = {"User-Agent": self.user_agent.random}
        proxy = get_proxies()
        page, last_page = 0,1
        while page<last_page:
            page+=1
            param = {'p': str(page), 'SI': city_n, 'ST': '1'} # 頁數、區域、排序方式(最新發布)
            try:
                s = requests.Session()
                res = s.get(url, headers=header, proxies=proxy, params=param, verify=False)
                soup = BeautifulSoup(res.text, 'lxml')
                items = soup.select_one('#SearchResultList').find_all('li',{'class':'col-12 col-xl-6'})
                for item in items:
                    link = item.find('a')['href']
                    source_id = re.findall('house/(\d+)', link)[0]
                    subject = item.find('h3').text.split('\n')[0].strip()
                    if subject == 'test': #居然有測試的放在上面..
                        continue
                    price = item.find('span',{'class':'object-price d-block'}).text.replace(',','')
                    if price == '可議價':# 可議價的話，先用-1
                        total = -1
                    else:
                        match = re.findall('[億|萬]?(\d+\.?\d*[億|萬|元])', price)
                        total = match[0]
                        if '億' in total:
                            total = int(float(total.rstrip('億')) * 10000)
                        elif '萬' in total:
                            total = int(float(total.rstrip('萬')))
                        elif '元' in total:
                            total = int(float(total.rstrip('元')) / 10000)
                        # 小於20萬的一定是單價
                        if total < 20:
                            match = re.findall('坪數:(\d+\.?\d*)坪', item.text)
                            ping = float(match[0]) if match else 1
                            total = int(total * ping)
                    obj_data = {
                                "source_id" : source_id,
                                "subject" : subject,
                                "link" : link,
                                "city" : city,
                                "re_price" : total,
                                "insert_time" : time.strftime("%Y-%m-%d %H:%M:%S")
                                }
                    list_obj.append(obj_data)

                pages = soup.select_one('#PageOption').find_all('li')
                if pages:
                    last_page = int(pages[-2].text.lstrip('...'))

            except Exception as e:
                self.logger.error(f"Error processing City{city} Page{page}: {str(e)}")
        
        # 將列表資訊都先寫入sqlite
        if list_obj:
            to_sqlite(self, list_obj)
        else: # 是空的也要建立空資料表
            con = sqlite3.connect(os.path.abspath('compare.db'))
            table_name = f'{self.name}_{time.strftime("%m%d")}'
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} (source_id, subject, link, city, re_price, insert_time)"
            con.execute(sql)
            con.close()
        
        # 寫到sqlite之後進行二次檢查，將價格介於20-100萬的物件，使用爬蟲進入物件頁面，取得正確總價total
        con = sqlite3.connect(os.path.abspath('compare.db'))
        cursor = con.cursor()
        table_name = f'{self.name}_{time.strftime("%m%d")}'
        # 抓出金額20～100萬ㄉ
        sql = f" SELECT source_id, link, re_price FROM {table_name} WHERE city = '{city}' AND re_price BETWEEN 20 AND 100 ; "
        check_total_links = pd.read_sql(sql, con=con)
        header = {"User-Agent": self.user_agent.random}
        proxy = get_proxies()
        for index, obj in check_total_links.iterrows():
            time.sleep(random.uniform(1,3))
            link = obj['link']
            source_id = obj['source_id']
            re_price = obj['re_price']
            try:
                res = requests.get(link, headers=header, proxies=proxy)
                soup = BeautifulSoup(res.text, 'lxml')
                price = soup.find('div',{'class':'Priceall'}).text
                # 是寫單價的，乘上坪數算出正確總價，並更改資料庫內容
                if '坪' in price:
                    info = soup.find('div',{'class':'row underline'}).text
                    match = re.findall('權狀坪數:(\d+\.?\d*)坪', info)
                    ping = float(match[0]) if match else 1
                    total = re_price * ping
                sql = f" UPDATE {table_name} SET re_price = {total} WHERE source_id = {source_id}"
                cursor.execute(sql)
            except Exception as e:
                self.logger.error(f"CheckTotalError processing City{city} link{link}: {str(e)}")
        con.close()

    def get_data(self, city_code):
        data_list = []
        list_obj = is_new(self, city_code)
        header = {"User-Agent": self.user_agent.random}
        proxy = get_proxies()
        for obj in list_obj:
            subject = obj['subject']
            link = obj['link']
            source_id = obj['source_id']
            city = obj['city']
            total = obj['re_price']
            print(subject, link)
            time.sleep(random.uniform(1,3))
            try:
                res = requests.get(link, headers=header, proxies=proxy)
                soup = BeautifulSoup(res.text, 'lxml')
                area = soup.select_one('#BreadcrumbBox > div > div > nav > ol > li:nth-child(3) > a').text
                pattern, house_age, house_age_v, total_ping, address, community = '', '', 0, 0, '', ''
                year, month = 0,0
                right_info = soup.find('div',{'class':'row underline'})
                subtitles = right_info.find_all('div',{'class':'col-5 Subtitle'})
                for i in range(len(subtitles)):
                    if subtitles[i].text == '權狀坪數:':
                        is_ping = right_info.find_all('div',{'class':'col-7 Subcontent'})[i].text.rstrip('坪')
                        if is_ping: # 少數會是空白..
                            total_ping = float(is_ping)
                    elif subtitles[i].text == '路名:':
                        address = right_info.find_all('div',{'class':'col-7 Subcontent'})[i].text.replace('\n','').replace('台','臺')
                    elif subtitles[i].text == '格局:':
                        pattern = right_info.find_all('div',{'class':'col-7 Subcontent'})[i].text
                    elif subtitles[i].text == '社區:':
                        community = right_info.find_all('div',{'class':'col-7 Subcontent'})[i].text
                    elif subtitles[i].text == '屋齡:':
                        house_age = right_info.find_all('div',{'class':'col-7 Subcontent'})[i].text
                        match = re.search(r'(\d+)年', house_age)
                        if match:
                            year = int(match.group(1))
                        match = re.search(r'(\d+)月', house_age)
                        if match:
                            month = int(match.group(1))
                        house_age_v = round((year + (month/12)),1)
                match = re.findall(f'{city}?{area}(.+)', address)
                if match:
                    road = match[0]
                else:
                    road = '' # 有可能會沒提供路名
                company = soup.find('span',{'class':'companyname'}).text.lstrip('經紀業：')
                mobile = soup.find('span',{'data-attr':'icon-mobil'})
                if mobile:
                    phone = mobile.text
                else:
                    tel = soup.find('span',{'data-attr':'icon-phone'})
                    if tel:
                        phone = tel.text
                lat, lng, img_url = 0, 0, ''
                map = soup.select_one('#googlemap')
                if map:
                    match = re.search('q=(\d+.\d+), (\d+.\d+)', map['src'])
                    if match:
                        lat = match.group(1)
                        lng = match.group(2)
                img = soup.find('ul', {'class':'pgwSlideshow'}).find('img')
                if img:
                    img_url = 'https://www.houseweb.com.tw' + img['data-src']
                situation = soup.select_one('#BreadcrumbBox > div > div > nav > ol > li:nth-child(4) > a').text
                house_type = soup.select_one('#BreadcrumbBox > div > div > nav > ol > li:nth-child(5) > a').text
                is_feature = soup.find('div',{'class':'special'})
                if is_feature:
                    feature = is_feature.text
                building_ping, att_ping, public_ping, land_ping = 0, 0, 0, 0
                floor_web, floor, total_floor = '', 0, 0
                blockto, manage_fee, parking_type = '', '', ''
                basic_info = soup.find('div',{'class':'container-fluid BasicInfo BackgroundGray'})
                infos = basic_info.find_all('li')
                for info in infos:
                    match = re.findall('主建物:(\d+\.*\d*)坪', info.text)
                    if match:
                        building_ping = float(match[0])
                    match = re.findall('附屬:(\d+\.*\d*)坪', info.text)
                    if match:
                        att_ping = float(match[0])
                    match = re.findall('公設:(\d+\.*\d*)坪', info.text)
                    if match:
                        public_ping = float(match[0])
                    match = re.findall('土地坪數:(\d+\.*\d*)坪', info.text)
                    if match:
                        land_ping = float(match[0])
                    match = re.findall('所在樓層:(\w+[-|+|~]*\d*樓)', info.text)
                    if match:
                        floor_web = match[0]
                        if 'B' in match[0]:
                            floor = -1
                        else:
                            match = re.findall('\d+', floor_web)
                            if match:
                                floor = int(match[0])
                    match = re.findall('建物規畫:(.+)', info.text)
                    if match:
                        build_floor = match[0]
                        match = re.findall('地上(\d+)層', build_floor)
                        if match:
                            total_floor = int(match[0])
                    match = re.findall('座向:(.+)', info.text)
                    if match:
                        blockto = match[0]
                    match = re.findall('管理費:(.+)', info.text)
                    if match:
                        manage_fee = match[0]
                # 車位資料先find'建物資訊'再找'車位'，才不會被'面積資料'的'車位'(坪數)蓋過去
                build_info = basic_info.find('div', {'class':'container categoryBG'}).find_all('li')
                for info in build_info:
                    match = re.findall('車位:(.+)', info.text)
                    if match:
                        parking_type = match[0]
                price_ave = 0
                price_single = soup.find('div',{'class':'col-12 col-md-6 PriceSingle'})
                if price_single:
                    match = re.findall('(\d+\.*\d*) 萬/坪', price_single.text)
                    if match:
                        price_ave = float(match[0])
                if price_ave == 0:
                    if total_ping!= 0:
                        price_ave = round(total/total_ping,2)
                contact_man = ''
                is_man = soup.find('div',{'class':'col-12 Memname'}).find('a')
                if is_man:
                    contact_man = is_man.text
            
            except Exception as e:
                self.logger.error(f"Error processing link {link}: {str(e)}")
                continue
            
            # brand跟branch有些寫在房仲姓名裡,有些寫在經紀業裡，且格式不統一
            contact_type = '仲介'
            brand, branch = '', ''
            
            # 此網站無提供以下資訊
            house_num, pattern1, edge, dark, manage_type = '', '', '', '', ''
            
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
        
        # 把物件都寫進資料庫之後，再寫入group_key跟address_cal
        find_group(self, city_code)
        find_possible_addrs(self, city_code)

city_code = 'C'
hw = HouseWeb()
hw.get_items(city_code)
hw.get_data(city_code)

# 更新舊資料
is_del(hw, city_code)
is_update(hw, city_code)