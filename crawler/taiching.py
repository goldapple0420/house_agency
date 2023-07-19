from dataclasses import dataclass
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re,time,json,math
import random
from fake_useragent import UserAgent
from crawler.craw_common import *

class Taiching():
    def __init__(self):
        self.name = 'taiching'
        self.source = '台慶不動產'
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
        city = self.all_city_dict[city_code]
        city_n = city.replace('臺','台')
        index_url = 'https://www.taiching.com.tw/region/Search/'
        proxy = get_proxies()
        page = 0
        while True:
            try:
                page+=1
                payload = {"PostData":'{\"City\":\"' + city_n + '\",\"Dist\":\"\",\"Price\":\",\",\"Pyeong\":\",\",\"PyeongType\":0,\"SearchFor\":\"all\",\"Page\":' + str(page) + ',\"Filiter\":\"\",\"Keywords\":\"\",\"HouseType\":[],\"HouseAge\":[],\"Rooms\":[],\"RoomsSP\":\"false\",\"Floors\":[]}'}
                header = {
                    'Host': 'www.taiching.com.tw',
                    'Origin': 'https://www.taiching.com.tw',
                    'Referer': 'https://www.taiching.com.tw/region/index',
                    'User-Agent': self.user_agent.random ,
                    }
                s = requests.session()
                time.sleep(random.uniform(3,6))
                res = s.post(index_url, json=payload, headers=header, proxies=proxy)
                house_list = res.json()
                print(f'開始擷取{self.source} {city} P{page} 物件')
                # 完全0物件
                if not house_list: 
                    break
                # 到下一頁看物件的RowNo才能得知已無其他物件，並重複停留在最後一頁（最後一頁也可能剛好30個物件）（可能全部物件都是台慶的，不會因為出現有巢氏物件而跳出迴圈）
                
                if house_list[-1]['RowNo'] < len(list_obj):
                    break
                for list in house_list:
                    if not list['CaseSID2']: # 有id2的來源為有巢氏房屋，不列入
                        list['re_price'] = int(float(list.pop('Price')))
                        list['subject'] = list.pop('CaseName')
                        list['source_id'] = str(list.pop('CaseSID'))
                        list['city'] = city
                        list['link'] = 'https://www.taiching.com.tw/house/detail/' + str(list['source_id'])
                        list_obj.append(list)
                    else:
                        # 只要出現非台慶網站物件，也代表已無台慶物件
                        if list_obj:
                            to_sqlite(self, list_obj)
                        else: # 是空的也要建立空資料表
                            con = sqlite3.connect(os.path.abspath('compare.db'))
                            table_name = f'{self.name}_{time.strftime("%m%d")}'
                            sql = f'''
                            CREATE TABLE IF NOT EXISTS {table_name} (
                                "RowNo"	INTEGER,
                                "County"	TEXT,
                                "District"	TEXT,
                                "Address"	TEXT,
                                "CaseDes"	TEXT,
                                "CaseTypeName"	TEXT,
                                "BuildAge"	REAL,
                                "LastPrice"	REAL,
                                "CarPrice"	TEXT,
                                "RegArea"	REAL,
                                "LandPin"	REAL,
                                "MainArea"	REAL,
                                "TotalAuxiArea"	REAL,
                                "PorchArea"	REAL,
                                "Platform"	REAL,
                                "GazeboArea"	REAL,
                                "ElevatorArea"	REAL,
                                "MezzanineArea"	REAL,
                                "AuxiArea"	REAL,
                                "PublicArea"	REAL,
                                "BasementArea"	REAL,
                                "CaseFromFloor"	REAL,
                                "CaseToFloor"	REAL,
                                "UpFloor"	REAL,
                                "Room"	REAL,
                                "LivingRoom"	REAL,
                                "BathRoom"	REAL,
                                "AddRoom"	TEXT,
                                "AddLivingRoom"	TEXT,
                                "AddBathRoom"	TEXT,
                                "PhotoList"	TEXT,
                                "PhotoPath"	TEXT,
                                "ShowPic"	TEXT,
                                "GoodSCase"	INTEGER,
                                "PublicCarArea"	TEXT,
                                "TotalRows"	INTEGER,
                                "PassDay"	INTEGER,
                                "CaseKey"	TEXT,
                                "IStagin"	INTEGER,
                                "MainShopHQID"	TEXT,
                                "CaseSID2"	TEXT,
                                "TotalPages"	INTEGER,
                                "PicUrl"	TEXT,
                                "SPC"	TEXT,
                                "SPCNum"	INTEGER,
                                "PicNum"	INTEGER,
                                "re_price"	INTEGER,
                                "subject"	TEXT,
                                "source_id"	TEXT,
                                "city"	TEXT,
                                "link"	TEXT
                                );
                            '''
                            con.execute(sql)
                            con.close()
                        return list_obj
            
            except requests.exceptions.ProxyError as e:
                self.logger.error(f"ProxyError processing City {city_code} Page {page}: {str(e)}")
                time.sleep(random.uniform(1,2))
                proxy = get_proxies()
                continue
            
            except requests.exceptions.SSLError as e:
                self.logger.error(f"SSLEError processing City {city_code} Page {page}: {str(e)}")
                time.sleep(random.uniform(3,11))
                proxy = get_proxies()
                continue
            
            except Exception as e:
                self.logger.error(f"Error processing City {city_code} Page {page}: {str(e)}")
                continue
        
        if list_obj:
            to_sqlite(self, list_obj)
        else: # 是空的也要建立空資料表
            con = sqlite3.connect(os.path.abspath('compare.db'))
            table_name = f'{self.name}_{time.strftime("%m%d")}'
            sql = f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                "RowNo"	INTEGER,
                "County"	TEXT,
                "District"	TEXT,
                "Address"	TEXT,
                "CaseDes"	TEXT,
                "CaseTypeName"	TEXT,
                "BuildAge"	REAL,
                "LastPrice"	REAL,
                "CarPrice"	TEXT,
                "RegArea"	REAL,
                "LandPin"	REAL,
                "MainArea"	REAL,
                "TotalAuxiArea"	REAL,
                "PorchArea"	REAL,
                "Platform"	REAL,
                "GazeboArea"	REAL,
                "ElevatorArea"	REAL,
                "MezzanineArea"	REAL,
                "AuxiArea"	REAL,
                "PublicArea"	REAL,
                "BasementArea"	REAL,
                "CaseFromFloor"	REAL,
                "CaseToFloor"	REAL,
                "UpFloor"	REAL,
                "Room"	REAL,
                "LivingRoom"	REAL,
                "BathRoom"	REAL,
                "AddRoom"	TEXT,
                "AddLivingRoom"	TEXT,
                "AddBathRoom"	TEXT,
                "PhotoList"	TEXT,
                "PhotoPath"	TEXT,
                "ShowPic"	TEXT,
                "GoodSCase"	INTEGER,
                "PublicCarArea"	TEXT,
                "TotalRows"	INTEGER,
                "PassDay"	INTEGER,
                "CaseKey"	TEXT,
                "IStagin"	INTEGER,
                "MainShopHQID"	TEXT,
                "CaseSID2"	TEXT,
                "TotalPages"	INTEGER,
                "PicUrl"	TEXT,
                "SPC"	TEXT,
                "SPCNum"	INTEGER,
                "PicNum"	INTEGER,
                "re_price"	INTEGER,
                "subject"	TEXT,
                "source_id"	TEXT,
                "city"	TEXT,
                "link"	TEXT
                );
            '''
            con.execute(sql)
            con.close()


    def get_data(self, city_code):
        list_obj = is_new(self, city_code)
        data_list = []
        for obj in list_obj:
            subject = obj['subject']
            source_id = obj['source_id']
            link = obj['link']
            print(subject, link)
            address = obj['Address'].replace('台','臺')
            house_age_v = obj['BuildAge']
            house_age = str(house_age_v) + '年'
            feature = obj['CaseDes']
            house_type = obj['CaseTypeName']
            city = obj['County'].replace('台','臺')
            area = obj['District']
            match = re.findall(f'{city}?{area}(.+)', address)
            road = match[0] if match else ''
            land_ping = obj['LandPin']
            building_ping = obj['MainArea']
            total_ping = obj['RegArea']
            public_ping = obj['PublicArea']
            att_ping = obj['TotalAuxiArea']
            total = obj['re_price']
            floors = obj['CaseFromFloor']
            total_floor = obj['UpFloor']
            if total_ping != 0:
                price_ave = round((total/total_ping),2)
            else:
                if land_ping != 0:
                    price_ave = round((total/land_ping),2)
                else:
                    price_ave = 0
            if not floors or (math.isnan(floors)):
                floor = 0
                floor_web = ''
            else:
                floor = int(floors)
                floor2 = obj['CaseToFloor']
                if floor2:
                    floor_web = f'{floors}~{floor2}'
                else: # 有floor沒floor2
                    floor_web = str(floors)
            if not total_floor or (math.isnan(total_floor)):
                total_floor = 0
            if not feature:
                feature = ''
            header = {'User-Agent': self.user_agent.random }
            time.sleep(random.uniform(1,6))
            try:
                if len(data_list) % 15 == 0: #每15筆換一個IP
                    proxy = get_proxies()
                res = requests.get(link, headers=header, proxies=proxy)
                link_soup = BeautifulSoup(res.text, 'lxml')
                # 格局
                is_pattern = link_soup.select_one("body > div > main > section.m-house-infomation > div.m-house-infos.right > div.m-house-info-wrap > div:nth-child(2) > div > span")
                pattern = ''
                if is_pattern:
                    pattern = is_pattern.text.replace('\n', '').replace(' ', '')
                # 其他資訊
                manage_type, manage_fee = '', ''
                info_detail_last = link_soup.find('section',{'class':'m-house-detail-list bg-other last'})
                if info_detail_last:
                    match = re.findall(r'(朝\s*.+)',info_detail_last.text.strip())
                    if match:
                        blockto = match[0]
                    else:
                        blockto = ''
                    manage = info_detail_last.find_all('li',{'class':'right'})
                    if len(manage) > 0 :
                        match = re.search(r"建物管理費：(.+)",manage[0].text.strip())
                        if match:
                            manage_fee = match.group(1).rstrip('(備註)')
                        else:
                            manage_type = manage[0].text.strip()
                    if len(manage) == 2 :
                        manage_type = manage[1].text.strip()
                park = link_soup.find('section',{'class':'m-house-detail-list bg-car'})
                if park:
                    parking_type = park.text.strip().lstrip('車位\n').rstrip('(備註)')
                else:
                    parking_type = ''
                try:
                    img_url = link_soup.find('meta',{'itemprop':'image'})['content']
                except:
                    img_url = ''
                try:
                    phone = link_soup.select_one("body > div > main > section.m-house-infomation > div.m-house-infos.right > div.m-info-tel > em").get_text()
                except:
                    phone = ''
                # 經緯度
                try:
                    lat = round(float(link_soup.select_one('#hiddenCenterLat').get_attribute_list('value')[0]),7)
                    lng = round(float(link_soup.select_one('#hiddenCenterLng').get_attribute_list('value')[0]),7)
                except:
                    lat, lng = 0,0
                # 房仲資料
                brand, branch,company  = '', '', ''
                is_company = link_soup.select_one("#CompanyName")
                if is_company:
                    company = is_company.text
                is_brand = link_soup.select_one("#ShopName")
                if is_brand:
                    brand, branch = is_brand.text.split('-')
                
            except requests.exceptions.ProxyError as err:
                self.logger.error(f"Proxy Error processing link {link}: {str(err)}")
                time.sleep(random.uniform(5,10))
                proxy = get_proxies()
                continue

            except Exception as e:
                self.logger.error(f"Error processing link {link}: {str(e)}")
                continue
            
            # 此網站無提供以下資訊
            situation, house_num, contact_type, contact_man, pattern1, edge, dark, community = '', '', '', '', '', '', '', ''
            
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
            if len(data_list) % 10 == 0:
                data2sql(data_list, city_code)
                data_list = []

        data2sql(data_list, city_code)
        # 把物件都寫進資料庫之後，再寫入group_key跟address_cal
        find_group(self, city_code)
        find_possible_addrs(self, city_code)