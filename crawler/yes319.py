import requests
from bs4 import BeautifulSoup
import pandas as pd
import re,time
from fake_useragent import UserAgent
import random
from crawler.craw_common import *

class Yes319():
    def __init__(self):
        self.name = 'yes319'
        self.source = 'yes319'
        self.user_agent = UserAgent()
        self.logger = WriteLogger().getLoggers()
        self.all_city_dict = {
        "A": {"city_c": "臺北市", "city_n": "21"},
        "B": {"city_c": "臺中市", "city_n": "42"},
        "C": {"city_c": "基隆市", "city_n": "24"},
        "D": {"city_c": "臺南市", "city_n": "62"},
        "E": {"city_c": "高雄市", "city_n": "72"},
        "F": {"city_c": "新北市", "city_n": "22"},
        "G": {"city_c": "宜蘭縣", "city_n": "39"},
        "H": {"city_c": "桃園市", "city_n": "34"},
        "I": {"city_c": "嘉義市", "city_n": "53"},
        "J": {"city_c": "新竹縣", "city_n": "35"},
        "K": {"city_c": "苗栗縣", "city_n": "37"},
        "M": {"city_c": "南投縣", "city_n": "49"},
        "N": {"city_c": "彰化縣", "city_n": "47"},
        "O": {"city_c": "新竹市", "city_n": "36"},
        "P": {"city_c": "雲林縣", "city_n": "55"},
        "Q": {"city_c": "嘉義縣", "city_n": "52"},
        "T": {"city_c": "屏東縣", "city_n": "87"},
        "U": {"city_c": "花蓮縣", "city_n": "38"},
        "V": {"city_c": "臺東縣", "city_n": "89"},
        "X": {"city_c": "澎湖縣", "city_n": "69"},
        "W": {"city_c": "金門連江", "city_n": "82"},
        "Z": {"city_c": "金門連江", "city_n": "82"}
        }

    def get_house_items(self, city_code):
        list_obj = []
        city_n = self.all_city_dict[city_code]['city_n']
        city = self.all_city_dict[city_code]['city_c']
        city_url = f'https://www.yes319.com/0{city_n}/search.php'
        header = {'User-Agent': self.user_agent.random }
        page = 0
        while True:
            if page%5 == 0: # 每5頁換一個IP
                proxy = get_proxies()
            page+=1
            param = f'page={page}&S01=1' # S01=1按更新日期排序
            time.sleep(random.uniform(1,15))
            try:
                res = requests.get(city_url, params=param, headers=header, proxies=proxy)
                if '找不到' in res.text:
                    break
                soup = BeautifulSoup(res.text, 'lxml')
                print(f'開始擷取{self.source} {city} 房屋 P{page} 物件')
                # 2~29才是我們要的物件
                for i in range(2, 30):
                    item = soup.select_one(f'body > div.container > main > article > div.col-xs-12.col-sm-12.col-md-9.padding-default > div:nth-child(2) > div:nth-child({str(i)})')
                    is_obj = item.find('div',{'class':'item-title'})
                    if is_obj: # 一頁可能沒滿28個物件
                        link = 'https://www.yes319.com' + item.find('a')['href']
                        subject = is_obj.text
                        source_id = re.findall('objno=(.+)', link)[0]
                        re_price = item.find('div',{'class':'obj-money'}).text.rstrip('萬').replace(',','')
                        if '億' in re_price:
                            re_price = float(re_price.rstrip('億')) * 10000
                        else:
                            re_price = float(re_price)
                        # 金門連江要分出來
                        if city == '金門連江':
                            address = item.find('div',{'class':'item-data'}).text
                            # 連江縣只有南竿 北竿 莒光 東引
                            if ('竿' in address) or ('東引' in address) or ('莒光' in address):
                                city = '連江縣'
                            else:
                                city = '金門縣'
                        obj_data = {
                                    "source_id" : source_id,
                                    "subject" : subject,
                                    "link" : link,
                                    "city" : city,
                                    "re_price" : int(re_price),
                                    "insert_time" : time.strftime("%Y-%m-%d %H:%M:%S")
                                    }
                        list_obj.append(obj_data)
                    else: # 不滿28個物件，為最後一頁
                        if list_obj:
                            to_sqlite(self, list_obj)
                        else: # 是空的也要建立空資料表
                            con = sqlite3.connect(os.path.abspath('compare.db'))
                            table_name = f'{self.name}_{time.strftime("%m%d")}'
                            sql = f"CREATE TABLE IF NOT EXISTS {table_name} (source_id, subject, link, city, re_price, insert_time)"
                            con.execute(sql)
                            con.close()
                        return
            
            except requests.exceptions.ProxyError as err:
                self.logger.error(f"Proxy Error processing link {link}: {str(err)}")
                time.sleep(random.uniform(15,30))
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

    def get_land_items(self, city_code):
        list_obj = []
        city_n = self.all_city_dict[city_code]['city_n']
        city = self.all_city_dict[city_code]['city_c']
        city_url = f'https://www.yes319.com/0{city_n}land/search.php'
        header = {'User-Agent': self.user_agent.random }
        page = 0
        while True:
            if page%5 == 0: # 每5頁換一個IP
                proxy = get_proxies()
            page+=1
            param = f'page={page}&S01=1' # S01=1按更新日期排序
            time.sleep(random.uniform(1,15))
            try:
                res = requests.get(city_url, params=param, headers=header, proxies=proxy)
                if '找不到' in res.text: # 當頁完全沒物件
                    # print(f'{self.source} {city}共{len(list_obj)}個物件')
                    break
                soup = BeautifulSoup(res.text, 'lxml')
                print(f'開始擷取{self.source} {city} 土地 P{page} 物件')
                # 2~29才是我們要的物件
                for i in range(2, 30):
                    item = soup.select_one(f'body > div.container > main > article > div.col-xs-12.col-sm-12.col-md-9.padding-default > div:nth-child(2) > div:nth-child({str(i)})')
                    is_obj = item.find('div',{'class':'item-title'})
                    if is_obj: # 一頁可能沒滿28個物件
                        link = 'https://www.yes319.com' + item.find('a')['href']
                        subject = is_obj.text
                        source_id = re.findall('objno=(.+)', link)[0]
                        price = item.find('div',{'class':'obj-money'}).text.replace(',','')
                        match = re.findall('(\d+\.?\d*[億|萬]) ', price)
                        re_price = match[0]
                        if '億' in re_price:
                            re_price = float(re_price.rstrip('億')) * 10000
                        else:
                            re_price = float(re_price.rstrip('萬'))
                        # 金門連江要分出來
                        if city == '金門連江':
                            address = item.find('div',{'class':'item-data'}).text
                            # 連江縣只有南竿 北竿 莒光 東引
                            if ('竿' in address) or ('東引' in address) or ('莒光' in address):
                                city = '連江縣'
                            else:
                                city = '金門縣'
                        obj_data = {
                                    "source_id" : source_id,
                                    "subject" : subject,
                                    "link" : link,
                                    "city" : city,
                                    "re_price" : int(re_price),
                                    "insert_time" : time.strftime("%Y-%m-%d %H:%M:%S")
                                    }
                        list_obj.append(obj_data)
                    else: # 不滿28個物件，為最後一頁
                        if list_obj:
                            to_sqlite(self, list_obj)
                        else: # 是空的也要建立空資料表
                            con = sqlite3.connect(os.path.abspath('compare.db'))
                            table_name = f'{self.name}_{time.strftime("%m%d")}'
                            sql = f"CREATE TABLE IF NOT EXISTS {table_name} (source_id, subject, link, city, re_price, insert_time)"
                            con.execute(sql)
                            con.close()
                        return
            
            except requests.exceptions.ProxyError as err:
                self.logger.error(f"Proxy Error processing link {link}: {str(err)}")
                time.sleep(random.uniform(15,30))
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
        data_list = []
        list_obj = is_new(self, city_code)
        for obj in list_obj:
            try:
                subject = obj['subject']
                link = obj['link']
                source_id = obj['source_id']
                city = obj['city']
                total = obj['re_price']
                print(subject, link)
                if len(data_list) % 14 == 0 : # 一頁共28個物件，抓超過一半就換一個IP
                    proxy = get_proxies()
                header = {'User-Agent': self.user_agent.random }
                time.sleep(random.uniform(3,16))    
                res = requests.get(link, headers=header, proxies=proxy)
                soup = BeautifulSoup(res.text, 'lxml')
                lat = soup.find('input',{'id':'HD93'})['value']
                lng = soup.find('input',{'id':'HD92'})['value']
                try:
                    total_ping = float(soup.find('input',{'id':'casePin'})['value'].replace(',','').rstrip('坪'))
                except:
                    total_ping = 0
                img = soup.select_one('#obj-img-carusel > div.carousel-inner > div.item.active > a > div > img')
                if img:
                    img_url = 'https://www.yes319.com' + img['src']
                else:
                    img_url = ''
                floor_web ,floor, total_floor = '', 0, 0
                house_type, pattern, blockto, address, parking_type, community, manage_fee, feature = '', '', '', '', '', '', '', ''
                building_ping, att_ping, land_ping, public_ping = 0,0,0,0
                house_age, house_age_v = '', 0
                contact_man, brand, branch, phone, company = '', '', '', '', ''
                
                # 土地及房屋的欄位格式不同
                infos = soup.find_all('div',{'class':'data-set'})
                if 'land' not in link:
                    for info in infos:
                        if info.find('div',{'class':'column-title'}):
                            if info.find('div',{'class':'column-title'}).text == '樓層/樓高':
                                floors = info.find('div',{'class':'column-data'}).text
                                if '/' in floors:
                                    try:
                                        floor_web, total_f = floors.split('/')
                                    except:
                                        floor_web = floors.split('/')[0]
                                        match = re.findall('\d+',floors)
                                        if match:
                                            total_floor = float(match[-1])
                                    else:
                                        match = re.findall('\d+',total_f)
                                        if match:
                                            total_floor = int(match[0])
                                        if 'B' in floor_web:
                                            floor = -1
                                        else:
                                            match = re.findall('\d+',floor_web)
                                        if match:
                                            floor = int(match[0])
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
                                feature = info.find('div',{'class':'column-data'}).get_text(strip=True)
                            elif info.find('div',{'class':'column-title'}).text == '主建物坪數':
                                building_pings = info.find('div',{'class':'column-data'}).text.replace(',','').rstrip('坪')
                                if building_pings:
                                    building_ping  =float(building_pings)
                            elif info.find('div',{'class':'column-title'}).text == '附屬建物坪數':
                                att_pings = info.find('div',{'class':'column-data'}).text.replace(',','').rstrip('坪')
                                if att_pings:
                                    att_ping = float(att_pings)
                            elif info.find('div',{'class':'column-title'}).text == '土地坪數':
                                land_pings = info.find('div',{'class':'column-data'}).text.replace(',','').rstrip('坪')
                                if land_pings:
                                    land_ping = float(land_pings)
                            elif info.find('div',{'class':'column-title'}).text == '公共設施坪數':
                                public_pings = info.find('div',{'class':'column-data'}).text.replace(',','').rstrip('坪')
                                if public_pings:
                                    public_ping = float(public_pings)
                            elif info.find('div',{'class':'column-title'}).text == '建築完成日期':
                                age = info.find('div',{'class':'column-data'}).text.strip()
                                if age:
                                    house_age = re.findall(r'\d+年', age)[-1]
                                    house_age_v = int(house_age.rstrip('年'))
                            elif info.find('div',{'class':'column-title'}).text == '聯絡人員':
                                contact_man = info.find('div',{'class':'column-data'}).text.strip()
                            elif info.find('div',{'class':'column-title'}).text == '所屬公司':
                                company = info.find('div',{'class':'column-data'}).text.strip()
                            elif info.find('div',{'class':'column-title'}).text == '品牌名稱':
                                brand_name = info.find('div',{'class':'column-data'}).text
                                if '-' in brand_name:
                                    brand =  brand_name.split('-')[0]
                                    branch = brand_name.lstrip(brand).lstrip('-')
                                else:
                                    brand = brand_name
                                    branch = brand_name
                            elif info.find('div',{'class':'column-title'}).text == '電話(一)':
                                phone = re.findall('\(*\d+\)*\-*\d+\-*\d+',info.find('div',{'class':'column-data'}).text.strip())[0]
                else:
                    for info in infos:
                        if info.find('div',{'class':'column-title'}):
                            if info.find('div',{'class':'column-title'}).text == '土地類型':
                                house_type = info.find('div',{'class':'column-data'}).text.strip()
                            elif info.find('div',{'class':'column-title'}).text == '朝向':
                                blockto = info.find('div',{'class':'column-data'}).text.strip()
                            elif info.find('div',{'class':'column-title'}).text == '位置':
                                address = info.find('div',{'class':'column-data'}).text.strip().replace('台','臺')
                            elif info.find('div',{'class':'column-title'}).text == '特色':
                                feature = info.find('div',{'class':'column-data'}).text.strip()
                            elif info.find('div',{'class':'column-title'}).text == '聯絡人員':
                                contact_man = info.find('div',{'class':'column-data'}).text.strip()
                            elif info.find('div',{'class':'column-title'}).text == '所屬公司':
                                company = info.find('div',{'class':'column-data'}).text.strip()
                            elif info.find('div',{'class':'column-title'}).text == '品牌名稱':
                                brand_name = info.find('div',{'class':'column-data'}).text
                                if '-' in brand_name:
                                    brand =  brand_name.split('-')[0]
                                    branch = brand_name.lstrip(brand).lstrip('-')
                                else:
                                    brand = brand_name
                                    branch = brand_name
                            elif info.find('div',{'class':'column-title'}).text == '電話(一)':
                                phone = re.findall('\(*\d+\)*\-*\d+\-*\d+',info.find('div',{'class':'column-data'}).text.strip())[0]
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
            except requests.exceptions.ProxyError as err:
                self.logger.error(f"Proxy Error processing link {link}: {str(err)}")
                time.sleep(random.uniform(15,30))
                proxy = get_proxies()
                continue

            except Exception as e:
                self.logger.error(f"Error processing link {link}: {str(e)}")
                continue
            
            # 此網站無提供以下資訊
            situation, pattern1, house_num, edge, dark, manage_type, contact_type = '', '', '', '', '', '', ''

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