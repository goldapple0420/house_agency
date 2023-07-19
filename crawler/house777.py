import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import json
import time
import random
import pandas as pd
import re
from crawler.craw_common import *

# 此網站皆為高雄房仲物件，物件地區大多位於高雄(也有少數其他縣市)
# 物件url會太長所以寫入資料庫的link為部份
class House777():
    def __init__(self):
        self.name = 'house777'
        self.source = '777房仲網'
        self.user_agent = UserAgent()
        self.logger = WriteLogger().getLoggers()
        self.all_city_dict = {
        "A": {"city_c": "臺北市", "city_n": "021"},
        "B": {"city_c": "臺中市", "city_n": "042"},
        "C": {"city_c": "基隆市", "city_n": "024"},
        "D": {"city_c": "臺南市", "city_n": "062"},
        "E": {"city_c": "高雄市", "city_n": "072"},
        "F": {"city_c": "新北市", "city_n": "022"},
        "G": {"city_c": "宜蘭縣", "city_n": "039"},
        "H": {"city_c": "桃園市", "city_n": "034"},
        "I": {"city_c": "嘉義市", "city_n": "053"},
        "J": {"city_c": "新竹縣", "city_n": "035"},
        "K": {"city_c": "苗栗縣", "city_n": "037"},
        "M": {"city_c": "南投縣", "city_n": "049"},
        "N": {"city_c": "彰化縣", "city_n": "047"},
        "O": {"city_c": "新竹市", "city_n": "036"},
        "P": {"city_c": "雲林縣", "city_n": "055"},
        "Q": {"city_c": "嘉義縣", "city_n": "052"},
        "T": {"city_c": "屏東縣", "city_n": "087"},
        "U": {"city_c": "花蓮縣", "city_n": "038"},
        "V": {"city_c": "臺東縣", "city_n": "089"},
        "X": {"city_c": "澎湖縣", "city_n": "069"},
        "W": {"city_c": "金門縣", "city_n": "082"},
        "Z": {"city_c": "連江縣", "city_n": "082"}
        }

    def get_house_items(self, city_code):
        list_obj = []
        city_n = self.all_city_dict[city_code]['city_n']
        city = self.all_city_dict[city_code]['city_c']
        page = 0
        while True:
            if page % 10 == 0: # 每跑10頁換一個IP
                proxy = get_proxies()
            header = {'User-Agent': self.user_agent.random }
            page+=1
            url = f'https://www.house777.com.tw/m2/index.php?page={page}&M2A1={city_n}'
            time.sleep(random.uniform(1,3))
            try:
                res = requests.get(url, headers=header, proxies=proxy)
                res.raise_for_status()
                soup = BeautifulSoup(res.text,'lxml')
                item_block = soup.find('td',{'style':'padding:0px 0px 0px 5px;'})
                items = item_block.find_all('a',{'class':'link_c'})
                if not items:
                    # print(f'City{city_code} 總共{len(list_obj)}個物件')
                    break
                for item in items:
                    real_link = 'https://www.house777.com.tw/m2/' + item['href']
                    link = real_link.replace(f'&t=HD&M2A1={city_n}&M2S1=&M2S1b=&M2S2=&M2S3B=&M2S3E=&M2S4=&M2S4b=&M2S5B=&M2S5E=&M2S6B=&M2S6E=','')
                    subject = item.find('div',{'class':'font5'}).get_text(strip=True)
                    id = 'm2' + re.findall('A3=(.+)', link)[0] # 沒物件source_id, 自己組
                    re_price = int(item.find_all('span')[1].text.replace(',',''))
                    obj_data = {
                                "source_id" : id,
                                "subject" : subject,
                                "link" : link,
                                "real_link" : real_link,
                                "city" : city,
                                "re_price" : int(re_price),
                                "insert_time" : time.strftime("%Y-%m-%d %H:%M:%S")
                                }
                    list_obj.append(obj_data)
            except Exception as e:
                self.logger.error(f"Error processing House City{city_code} Page{page}: {str(e)}")        
        
        if list_obj:
            to_sqlite(self, list_obj)
        else: # 是空的也要建立空資料表
            con = sqlite3.connect(os.path.abspath('compare.db'))
            table_name = f'{self.name}_{time.strftime("%m%d")}'
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} (source_id, subject, link, city, re_price, insert_time, real_link)"
            con.execute(sql)
            con.close()


    def get_land_items(self, city_code):
        list_obj = []
        page = 0
        city_n = self.all_city_dict[city_code]['city_n']
        city = self.all_city_dict[city_code]['city_c']
        while True:
            if len(list_obj) % 10 == 0: # 每跑10頁換一個IP
                proxy = get_proxies()
            header = {'User-Agent': self.user_agent.random }
            page+=1
            url = f'https://www.house777.com.tw/m3/index.php?page={page}&M3A1={city_n}'
            time.sleep(random.uniform(3,9))
            try:
                res = requests.get(url, headers=header, proxies=proxy)
                res.raise_for_status()
                soup = BeautifulSoup(res.text,'lxml')
                item_block = soup.find('td',{'style':'padding:0px 0px 0px 5px;'})
                items = item_block.find_all('a',{'class':'link_c'})
                if not items:
                    # print(f'City{city_code} 總共{len(list_obj)}個物件')
                    break
                for item in items:
                    real_link = 'https://www.house777.com.tw/m3/' + item['href']
                    link = real_link.replace(f'&M3A1={city_n}&M3S1=&M3S2=&M3S2b=&M3S3B=&M3S3E=&M3S4=&M3S1b=&M3S5B=&M3S5E=','')
                    link = link.replace('https://www.','')
                    subject = item.find('div',{'class':'font5'}).get_text(strip=True)
                    id = 'm3' + re.findall('A3=(.+)', link)[0] # 沒物件source_id, 自己組
                    re_price = int(float(item.find_all('span')[1].text.replace(',','')))
                    obj_data = {
                                "source_id" : id,
                                "subject" : subject,
                                "link" : link,
                                "real_link" : real_link,
                                "city" : city,
                                "re_price" : int(re_price),
                                "insert_time" : time.strftime("%Y-%m-%d %H:%M:%S")
                                }
                    list_obj.append(obj_data)
            except Exception as e:
                self.logger.error(f"Error processing Land City{city_code} Page{page}: {str(e)}")        
        
        if list_obj:
            to_sqlite(self, list_obj)
        else: # 是空的也要建立空資料表
            con = sqlite3.connect(os.path.abspath('compare.db'))
            table_name = f'{self.name}_{time.strftime("%m%d")}'
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} (source_id, subject, link, city, re_price, insert_time, real_link)"
            con.execute(sql)
            con.close()

    def get_data(self, city_code):
        list_obj = is_new(self, city_code)
        data_list = []
        for obj in list_obj:
            try:
                subject = obj['subject']
                link = obj['link']
                city = obj['city']
                total = obj['re_price']
                source_id = obj['source_id']
                real_link = obj['real_link']
                print(subject, real_link)
                if len(data_list) % 10 == 0: # 每爬10個換一個IP
                    proxy = get_proxies()
                header = {'User-Agent': self.user_agent.random }
                time.sleep(random.uniform(10,15))
                res = requests.get(real_link, headers=header, proxies=proxy)
                soup = BeautifulSoup(res.text,'lxml')
                subject = soup.select_one('body > div.container > div:nth-child(4) > div.article-right.col-lg-8.col-md-10.col-sm-9.col-xs-12 > fieldset:nth-child(5) > legend > span.font6').text
                # source_id = soup.select_one('body > div > div:nth-child(4) > div.article-right.col-lg-8.col-md-10.col-sm-9.col-xs-12 > fieldset:nth-child(5) > legend > span.font4').text
                feature = soup.find('meta',{'name':'description'})['content'].strip()
                img = soup.find('meta',{'property':'og:image'})
                img_url = img['content'] if img else ''
                floor_web ,floor, total_floor = '', 0, 0
                house_type, pattern, blockto, address, parking_type, community, manage_fee, feature = '', '', '', '', '', '', '', ''
                total_ping, building_ping, att_ping, land_ping, public_ping = 0,0,0,0,0
                house_age, house_age_v = '', 0
                contact_man, brand, branch, phone, company = '', '', '', '', ''
                infos = soup.find_all('div',{'class':'data-set'})
                # 建物的資料欄位格式
                if 'm2/index.' in real_link:
                    for info in infos:
                        if info.find('div',{'class':'column-title'}):
                            if info.find('div',{'class':'column-title'}).text == '樓層/樓高':
                                floors = info.find('div',{'class':'column-data'}).text
                                if '/' in floors:
                                    try:
                                        floor_web, total_floors = floors.split('/')
                                    except:
                                        floor_web = floors.split('/')[0]
                                        match = re.findall('\d+',floors)
                                        if match:
                                            total_floor = float(match[-1])
                                    else:
                                        match = re.findall('\d+',total_floors)
                                        if match:
                                            total_floor = int(match[0])
                                        if 'B' in floor_web:
                                            floor = -1
                                        else:
                                            match = re.findall('\d+',floor_web)
                                        if match:
                                            floor = int(match[0])
                            elif info.find('div',{'class':'column-title'}).text == '建物總登坪數':
                                total_ping = info.find('div',{'class':'column-data'}).text.strip().replace(',','').rstrip('坪')
                                if total_ping == '未保存登記':
                                    total_ping = 0
                                elif total_ping:
                                    total_ping = float(total_ping)
                                else:
                                    total_ping = 0
                            elif info.find('div',{'class':'column-title'}).text == '房屋類型':
                                house_type = info.find('div',{'class':'column-data'}).text.strip()
                            elif info.find('div',{'class':'column-title'}).text == '格局':
                                pattern = info.find('div',{'class':'column-data'}).text.strip()
                            elif info.find('div',{'class':'column-title'}).text == '朝向':
                                blockto = info.find('div',{'class':'column-data'}).text.strip()
                            elif info.find('div',{'class':'column-title'}).text == '地址':
                                address = info.find('div',{'class':'column-data'}).text.strip().replace('台','臺')
                            elif info.find('div',{'class':'column-title'}).text == '車位':
                                parking_type = info.find('div',{'class':'column-data'}).text.strip()
                            elif info.find('div',{'class':'column-title'}).text == '社區名稱':
                                community = info.find('div',{'class':'column-data'}).text.strip()
                            elif info.find('div',{'class':'column-title'}).text == '管理費':
                                manage_fee = info.find('div',{'class':'column-data'}).text.strip()
                            elif info.find('div',{'class':'column-title'}).text == '特色':
                                feature = info.find('div',{'class':'column-data'}).text.strip()
                            elif info.find('div',{'class':'column-title'}).text == '主建物坪數':
                                is_building_ping = info.find('div',{'class':'column-data'}).text.replace(',','').rstrip('坪')
                                if is_building_ping:
                                    building_ping  =float(is_building_ping)
                            elif info.find('div',{'class':'column-title'}).text == '附屬建物坪數':
                                is_att_ping = info.find('div',{'class':'column-data'}).text.replace(',','').rstrip('坪')
                                if is_att_ping:
                                    att_ping = float(is_att_ping)
                            elif info.find('div',{'class':'column-title'}).text == '土地坪數':
                                is_land_ping = info.find('div',{'class':'column-data'}).text.replace(',','').rstrip('坪')
                                if is_land_ping:
                                    land_ping = float(is_land_ping)
                            elif info.find('div',{'class':'column-title'}).text == '公共設施坪數':
                                is_public_ping = info.find('div',{'class':'column-data'}).text.replace(',','').rstrip('坪')
                                if is_public_ping:
                                    public_ping = float(is_public_ping)
                            elif info.find('div',{'class':'column-title'}).text == '建築完成日':
                                age = info.find('div',{'class':'column-data'}).text.strip()
                                if age:
                                    house_age = re.findall(r'\d+年', age)[-1]
                                    house_age_v = int(house_age.rstrip('年'))
                            elif info.find('div',{'class':'column-title'}).text == '服務人員':
                                contact = info.find('div',{'class':'column-data'}).text.strip()
                                match = re.findall('服務專線\:?(.+)', contact)
                                if match:
                                    phone = match[0]
                                match = re.findall('高雄.+店', contact)
                                if match:
                                    branch = match[0]
                                    brand = re.findall(rf'([\u4e00-\u9fff]+){branch}', contact)[0]
                                else:
                                    match = re.findall(r'[\u4e00-\u9fff]+房屋', contact)
                                    if match:
                                        brand = match[0]
                                        branch = re.findall(f'{brand}(.+)', contact)[0].strip()
                                # 如果還是抓不到brand跟branch
                                if not brand:
                                    brand = contact.split('\n')[0]
                            elif info.find('div',{'class':'column-title'}).text == '所屬公司':
                                company = info.find('div',{'class':'column-data'}).text.strip()
                # 剩下是土地
                else:
                    for info in infos:
                        if info.find('div',{'class':'column-title'}):
                            if info.find('div',{'class':'column-title'}).text == '坪數':
                                land_ping = info.find('div',{'class':'column-data'}).text.strip().replace(',','').rstrip('坪')
                                if land_ping == '未保存登記':
                                    land_ping = 0
                                elif land_ping:
                                    land_ping = float(land_ping)
                                else:
                                    land_ping = 0
                            elif info.find('div',{'class':'column-title'}).text == '類別':
                                house_type = info.find('div',{'class':'column-data'}).text.strip()
                            elif info.find('div',{'class':'column-title'}).text == '位置':
                                address = info.find('div',{'class':'column-data'}).text.strip().replace('台','臺')
                            elif info.find('div',{'class':'column-title'}).text == '特色':
                                feature = info.find('div',{'class':'column-data'}).text.strip()
                            elif info.find('div',{'class':'column-title'}).text == '服務人員':
                                contact = info.find('div',{'class':'column-data'}).text.strip()
                                match = re.findall('服務專線\:?(.+)', contact)
                                if match:
                                    phone = match[0]
                                match = re.findall('高雄.+店', contact)
                                if match:
                                    branch = match[0]
                                    brand = re.findall(rf'([\u4e00-\u9fff]+){branch}', contact)[0]
                                else:
                                    match = re.findall(r'[\u4e00-\u9fff]+房屋', contact)
                                    if match:
                                        brand = match[0]
                                        branch = re.findall(f'{brand}(.+)', contact)[0].strip()
                                # 如果還是抓不到brand跟branch
                                if not brand:
                                    brand = contact.split('\n')[0]
                            elif info.find('div',{'class':'column-title'}).text == '所屬公司':
                                company = info.find('div',{'class':'column-data'}).text.strip()
                if total_ping!= 0:
                    price_ave = round(total/total_ping,2)
                else:
                    if land_ping!=0:
                        price_ave = round(total/land_ping,2)
                    else:
                        price_ave = 0
                area, road = '', ''
                if address:
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
                lat = float(soup.find('input',{'name':'HD93'})['value'])
                lng = float(soup.find('input',{'name':'HD92'})['value'])
            
            except requests.exceptions.ProxyError as err:
                self.logger.error(f"Proxy Error processing link {link}: {str(err)}")
                time.sleep(random.uniform(5,10))
                proxy = get_proxies()
                continue
            
            except Exception as err:
                self.logger.error(f"Error processing link {link}: {str(err)}")
                continue
            
            # 此網站無提供以下資訊
            situation, pattern1, house_num, edge, dark, manage_type, contact_type, contact_man = '', '', '', '', '', '', '', ''
            
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