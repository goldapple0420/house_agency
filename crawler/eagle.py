import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import re
import time
import random
from fake_useragent import UserAgent
from crawler.craw_common import *


class Eagle:
    def __init__(self):
        self.user_agent = UserAgent()
        self.name = 'eagle'
        self.source = '飛鷹地產'
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
    
    # 丟一個城市代碼進去,爬取該城市每頁的所有物件，蒐集基本資料，並回傳一個list_obj
    def get_items(self, city_code):
        list_obj = []
        city = self.all_city_dict[city_code]
        proxies = get_proxies()
        header = {'User-Agent': self.user_agent.random}
        page = 0
        while True:
            page+=1
            page_url = f'https://www.eagle111.com/mobile/ObjectMain.aspx?TP=1&ADR1={city}&Page={page}&OD=0'
            try:
                time.sleep(random.uniform(1,2))
                res = requests.get(page_url, headers=header, proxies=proxies)
                soup = BeautifulSoup(res.text, 'lxml')
                items = soup.find_all('div',{'class':'productItem'})
                if not items:
                    print(f'{self.name}City{city_code} 總共{len(list_obj)}個物件')
                    break
                for item in items:
                    link = 'https://www.eagle111.com/mobile/' + item.find('a')['href']
                    source_id = re.findall(r'sid=&IDX=(\d+)', link)[0]
                    subject = item.find('span',{'class':'houseTitle'}).get_text(strip=True)
                    price = item.find('span',{'class':'price'}).text.strip()
                    match = re.findall('\d+', price)
                    re_price = int(match[0])
                    obj_data = {
                        "source_id" : source_id,
                        "subject" : subject,
                        "link" : link,
                        "city" : city,
                        "re_price" : int(re_price),
                        "insert_time" : time.strftime("%Y-%m-%d %H:%M:%S")
                        }
                    list_obj.append(obj_data)

            except requests.exceptions.ProxyError as e:
                print(f'請求{self.name} URL:{page_url}失敗 Proxy:{proxies} ：', e)
                self.logger.error(f"ProxyError Source {self.source} Page {page} : {str(e)}")
                proxies = get_proxies()
                header = {'User-Agent': self.user_agent.random}

            except Exception as e:
                print(f'擷取{self.name} {city}第{page}頁清單失敗：', e)
                self.logger.error(f"Error processing Source {self.source} City {city_code} Page {page}: {str(e)}")
                continue

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
        proxies = get_proxies()
        header = {'User-Agent': self.user_agent.random}
        for obj in list_obj:
            time.sleep(random.uniform(1,2))
            subject = obj['subject']
            link = obj['link']
            source_id = obj['source_id']
            city = obj['city']
            total = obj['re_price']
            print(subject, link)
            try:
                res = requests.get(link, headers=header, proxies=proxies)
                if '查無此物件' in res.text:
                    print('此物件已不存在')
                    continue
                soup = BeautifulSoup(res.text, 'lxml')
                iteminfo = soup.find('table',{'class':'itemInfo'})
                address = re.findall('地　　址:\s+(.+?)\n', iteminfo.text)[0].strip().replace('台','臺')
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
                total_ping = float(re.findall(r'坪　　數:\s+(.+?)\s*坪', iteminfo.text)[0])
                price_ave = float(re.findall(r'單　　價:\s+(\d+(\.\d+)?)萬元',iteminfo.text)[0][0])
                house_type = re.findall('建物型態:\s+(.+?)\n', iteminfo.text)[0]
                match = re.findall(r'格　　局:\s+(.+?)\s+樓別/樓高', iteminfo.text)
                pattern = match[0].strip() if match else ''
                
                # 樓層
                floor_web, floor, total_floor = '', 0,0
                match = re.search(r'樓別/樓高:\s+(\w+\~?\w*樓)(?:\s*\/\s*(\w+\~?\w*樓))?', iteminfo.text)
                if match:
                    floor_web = match.group(1)
                    if floor_web:
                        floor_web = floor_web.rstrip('樓')
                        if '~' in floor_web:
                            floor = int(floor_web.split('~')[0])
                        else:
                            floor = int(floor_web)
                    total_floors = match.group(2)
                    if total_floors:
                        total_floor = total_floors.rstrip('樓')
                
                # 屋齡
                match = re.findall(r'屋　　齡:\s+(.+?)\s+朝　　向', iteminfo.text)
                if match:
                    house_age = match[0].strip()
                    match = re.search(r'(\d+)年(\d+)個月', house_age)
                    if match:
                        year = int(match.group(1))
                        month = int(match.group(2))
                    else:
                        year = int(re.search(r'(\d+)年', house_age).group(1))
                        month = 0
                    house_age_v = round((year + (month/12)),1)
                else:
                    house_age, house_age_v ='', 0
                
                # 座向、管理費、用途、特色
                match = re.findall(r'朝　　向:\s+(.+?)\s+管 理 費', iteminfo.text)
                blockto = match[0].strip() if match else ''
                
                match = re.findall(r'管 理 費:\s+(.+?)\s+土地使用分區', iteminfo.text)
                manage_fee = match[0].strip() if match else ''
                
                match = re.findall(r'土地使用分區:\s+(.+?)\s+建物結構', iteminfo.text)
                situation = match[0].strip() if match else ''
                
                match = re.findall(r'物件特色:\s+(.+)', iteminfo.text)
                feature = iteminfo.text.split('物件特色:')[-1].strip() if match else ''
                
                is_park = soup.select_one('#ContentPlaceHolder1_divHasCar')
                parking_type = is_park.text.strip() if is_park else ''
                
                # 坪數
                pings = soup.find('table',{'class':'itemInfo mobileItemInfo'})
                match = re.findall(r'主建物:\s+(.+?)坪', pings.text)
                building_ping = float(match[0].strip()) if match else 0
                match = re.findall(r'附屬建物:\s+(.+?)坪', pings.text)
                att_ping = float(match[0].strip()) if match else 0
                match = re.findall(r'公共設施\s+(.+?)坪', pings.text) # 公共設施沒冒號
                public_ping = float(match[0].strip()) if match else 0
                match = re.findall(r'土地坪數:\s+(.+?)坪', pings.text)
                land_ping = float(match[0].strip()) if match else 0
                
                # 圖片、經緯度
                img = soup.find('li',{'class':'itemImg'})
                img_url = img.find('img')['src'] if match else ''
                
                map = soup.select_one('#ContentPlaceHolder1_aCtopMap1')
                if map:
                    locate = map.get_attribute_list('href')[0]
                    lat = re.search(r'lat=([\d\.]+)&', locate).group(1)
                    lng = re.search(r'lng=([\d\.]+)&', locate).group(1)
                else:
                    lat, lng = 0,0
                
                # 房仲聯絡資訊
                contact_type = '房仲'
                agent = soup.find('div',{'class':'nameCardText'})
                shop = agent.select_one('#ContentPlaceHolder1_storeinfo').text.strip()
                brand, branch = shop.split('-')
                company = re.findall(f'{branch}\s*(.+?)\s+', agent.text)[0]
                phone = re.findall(f'{company}\s*(.+?)\s+', agent.text)[0]

            except Exception as e:
                logger = WriteLogger().getLoggers()
                logger.error(f"Error processing {self.source} link {link}: {str(e)}")
                continue

            # 此網站無提供以下資訊
            house_num, pattern1, contact_man, edge, dark, manage_type, community = '', '', '', '', '', '', ''

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