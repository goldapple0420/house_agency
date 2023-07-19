from dataclasses import dataclass
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re,time
import random
from fake_useragent import UserAgent
from crawler.craw_common import *

class HouseFun():
    def __init__(self):
        self.name = 'housefun'
        self.source = '好房網'
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
        city_c = all_city_dict[city_code].replace('臺','台')
        city = all_city_dict[city_code]
        proxy = get_proxies()
        page = 0
        while True:
            header = {'User-Agent': self.user_agent.random}
            page+=1
            index_url = f'https://buy.housefun.com.tw/region/{city_c}_c/?od=PostDateDown&pg={page}' # 依上架日期排序
            print(index_url)
            try:
                time.sleep(random.uniform(3,10))
                res = requests.get(index_url, headers=header, proxies=proxy)
                soup = BeautifulSoup(res.text, 'lxml')
                items = soup.find_all('section',{'class':'m-list-obj'})
                if not items:
                    break
                for item in items:
                    link = 'https://buy.housefun.com.tw' + item.find('a')['href']
                    source_id = re.findall('house/(\d+)',link)[0]
                    subject = item.find('h1').find('a').text
                    re_price = int(item.find('a',{'class':'discount-price'}).text.replace('萬', '').replace(',', '').strip())
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
                self.logger.error(f"Proxy Error processing {city} Page {page}: {str(err)}")
                time.sleep(random.uniform(5,10))
                proxy = get_proxies()

            except Exception as e:
                self.logger.error(f"Error processing {city} Page {page}: {str(e)}")

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
        for obj in list_obj:
            if len(data_list) % 10 == 0:
                proxy = get_proxies()
            source_id = obj['source_id']
            subject = obj['subject']
            subject = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9\s]+', '', subject)
            link = obj['link']
            total = obj['re_price']
            city = obj['city']
            print(subject, link)
            api_url = 'https://buy.housefun.com.tw/api/house'
            header = {"user-agent": self.user_agent.random}
            param = {'id': source_id }
            time.sleep(random.randint(1,3))
            try:
                res = requests.get(api_url, params=param, headers=header, proxies=proxy)
                item_data = res.json()['data'][0]
                address = item_data['address'].replace('台','臺')
                area = item_data['district']
                road = item_data['addrStreet']
                house_age = item_data['buildingAge']
                house_age_v = house_age
                feature = item_data['caseDescription']
                feature = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9\s]+', '', feature)
                house_type = item_data['caseTypeName']
                lat = item_data['latitude']
                lng = item_data['longitude']
                parking_type = item_data['parkingMode'] + item_data['parkingType']
                pattern = item_data['patternShow']
                situation = item_data['purposeShow']
                total_ping = item_data['regArea']
                blockto = item_data['dirLoca']
                community = item_data['community']['buildingName']
                manage_type = item_data['community']['manageType']
                is_manage_fee = item_data['community']['manageFee']
                manage_fee = '' if is_manage_fee == -1 else str(is_manage_fee) + '元/月'
                edge = item_data['sideRoom']
                contact_type = item_data['agent']['appellation']
                contact_man = item_data['agent']['name']
                brand = item_data['agent']['brand']
                branch = item_data['agent']['shop']
                company = item_data['agent']['company']
                phone = item_data['agent']['phone']
                floors = item_data['floorShow']
                floor_web, floor, total_floor = '', 0, 0
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
                try:
                    price_ave = float(item_data['unitPrice'].rstrip('萬/坪'))
                except:
                    price_ave = 0
                try:
                    img_url = 'https:' + item_data['pictures'][0]['url']
                except:
                    img_url = ''
                match = re.findall('(\d+\.*\d*) 坪',item_data['tranScript']['mainArea'])
                building_ping = float(match[0]) if match else 0
                match = re.findall('(\d+\.*\d*) 坪',item_data['tranScript']['landPin'])
                land_ping = float(match[0]) if match else 0
                match = re.findall('(\d+\.*\d*) 坪',item_data['tranScript']['publicArea'])
                public_ping = float(match[0]) if match else 0
                match = re.findall('(\d+\.*\d*) 坪',item_data['tranScript']['auxiArea'])
                other_ping = float(match[0]) if match else 0
                match = re.findall('(\d+\.*\d*) 坪',item_data['tranScript']['porchArea'])
                balcony_ping = float(match[0]) if match else 0
                att_ping = other_ping + balcony_ping
                if price_ave == 0:
                    try:
                        price_ave = total / total_ping
                    except:
                        price_ave = total / land_ping
            
            except requests.exceptions.ProxyError as err:
                self.logger.error(f"Error processing link {link}: {str(err)}")
                time.sleep(random.uniform(5,10))
                proxy = get_proxies()
                continue

            except Exception as err:
                self.logger.error(f"Error processing link {link}: {str(err)}")
                continue
            
            # 此網站無提供以下資訊
            pattern1, house_num, dark = '', '', ''
            
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