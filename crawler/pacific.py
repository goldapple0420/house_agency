import requests
from bs4 import BeautifulSoup
import pandas as pd
import re,time,json, math, sqlite3, os
from fake_useragent import UserAgent
import random
from crawler.craw_common import  ( get_proxies, to_sqlite, WriteLogger, is_update, is_new, data2sql, is_del, find_group, find_possible_addrs, clean_data)

class Pacific():
    def __init__(self):
        self.name = 'pacific'
        self.source = '太平洋房屋'
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
        city = self.all_city_dict[city_code]
        house_list = []
        url = 'https://www.pacific.com.tw/api/ObjectAPI/SearchObject2'
        header = {
        'User-Agent': self.user_agent.random ,
        'authorization':'Basic cHJtczpwcm1z'
        }
        proxy = get_proxies()
        s = requests.session()
        page = 0
        while True:
            page+=1
            data = {"Type":1,"CityID":city ,"Order":2,"DataType":0,"Page":page}
            time.sleep(random.uniform(5,11))
            res = s.post(url, json=data, headers=header, proxies=proxy)
            print(f'開始擷取{self.source} {city} P{page} 物件')
            res.raise_for_status()
            page_list = res.json()['lstData']
            total_count = res.json()['totalCount']
            if len(house_list)==total_count:
                break
            # print(f'{self.source} {city} P{page} {len(page_list)}個物件')
            for obj in page_list:
                source_id = obj['saleID']
                obj['source_id'] = source_id
                obj["subject"] = obj.pop('objectName')
                obj['link'] = 'https://www.pacific.com.tw/Object/ObjectDetail/?saleID=' + source_id
                obj['city'] = city
                obj['re_price'] = int(obj['sellTotalPrice'])
                house_list.append(obj)

        if house_list:
            to_sqlite(self, house_list)
        else: # 是空的也要建立空資料表
            con = sqlite3.connect(os.path.abspath('compare.db'))
            table_name = f'{self.name}_{time.strftime("%m%d")}'
            sql = f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                "company"	TEXT,
                "companyName"	TEXT,
                "idx"	INTEGER,
                "saleID"	TEXT,
                "hasSellAndRent"	INTEGER,
                "cityName"	TEXT,
                "areaName"	TEXT,
                "address"	TEXT,
                "sellTotalPrice"	REAL,
                "firstSellTotalPrice"	REAL,
                "layoutRoom"	REAL,
                "layoutHall"	REAL,
                "layoutToilet"	REAL,
                "totalArea"	REAL,
                "mainBuildArea"	REAL,
                "pubFacilityArea"	REAL,
                "outbuildingArea"	REAL,
                "ownerStallArea"	REAL,
                "pubStallArea"	REAL,
                "landArea"	REAL,
                "greenNum"	REAL,
                "promoted"	REAL,
                "hasStall"	INTEGER,
                "objectAge"	TEXT,
                "onWhichFloor"	TEXT,
                "buildingAboveFloor"	REAL,
                "maxFloor"	TEXT,
                "objectAttributType"	TEXT,
                "objectAttribut"	TEXT,
                "attributName"	TEXT,
                "x_POINT"	REAL,
                "y_POINT"	REAL,
                "objectCate"	INTEGER,
                "ageYear"	TEXT,
                "picCount"	INTEGER,
                "pubFacilityRange"	TEXT,
                "hasContractChange"	TEXT,
                "hasInConer"	TEXT,
                "objectStatus"	TEXT,
                "pic"	TEXT,
                "rentType"	TEXT,
                "rentTypeName"	TEXT,
                "direction"	TEXT,
                "directionName"	TEXT,
                "school1"	TEXT,
                "park"	TEXT,
                "market"	TEXT,
                "metro_RelationID"	TEXT,
                "totalDoorPerFloor"	TEXT,
                "elevatorPerFloor"	TEXT,
                "appointmentUrl"	TEXT,
                "msgUrl"	TEXT,
                "objectSearchType"	TEXT,
                "storeID"	TEXT,
                "lastUpdateDate"	TEXT,
                "innerWall"	TEXT,
                "externalWall"	TEXT,
                "structure"	TEXT,
                "saleEmployee"	TEXT,
                "aerialLink"	TEXT,
                "vrLink"	TEXT,
                "videoLink"	TEXT,
                "source_id"	TEXT,
                "subject"	TEXT,
                "link"	TEXT,
                "city"	TEXT,
                "re_price"	INTEGER
            );
            '''
            con.execute(sql)
            con.close()

    def get_data(self, city_code):
        house_list = is_new(self, city_code)
        data_list = []
        header = {'User-Agent': self.user_agent.random, 'authorization':'Basic cHJtczpwcm1z'}
        for house in house_list:
            if len(data_list) % 10 == 0:
                proxy = get_proxies()
            source_id = house['source_id']
            link = 'https://www.pacific.com.tw/Object/ObjectDetail/?saleID=' + source_id
            subject = house['subject']
            print(subject, link)
            img_url = house['pic']
            total = house['re_price']
            city = house['city']
            address = house['address'].strip().replace('台','臺')
            house_type = house['attributName']
            land_ping = house['landArea']
            building_ping = house['mainBuildArea']
            att_ping = house['outbuildingArea']
            public_ping = house['pubFacilityArea']
            total_ping = house['totalArea']
            lat = house['y_POINT']
            lng = house['x_POINT']
            total_floor = house['buildingAboveFloor']
            # 是None寫0
            if not total_floor:
                total_floor = 0
            else:
                # 不是None但是從資料庫讀出來是NaN也寫0
                if math.isnan(total_floor):
                    total_floor = 0
            floor_web = house['onWhichFloor']
            if 'B' in floor_web:
                floor = -1
            elif floor_web == '-99':  # 土地會標-99
                floor_web, floor = '', 0
            else:
                match = re.findall('-?\d+', floor_web)
                floor = match[0] if match else 0
            house_age = house['objectAge']
            if house_age:
                match = re.search(r'(\d+)年', house_age)
                year = int(match.group(1)) if match else 0
                match = re.search(r'(\d+)個月', house_age)
                month = int(match.group(1)) if match else 0.
                house_age_v = round((year + (month/12)),1)
            else:
                house_age = ''
                house_age_v = 0
            room = house['layoutRoom']
            if not room:
                room = 0
            else:
                room = 0 if math.isnan(room) else room
            hall = house['layoutHall']
            if not hall:
                hall = 0
            else:
                hall = 0 if math.isnan(hall) else hall
            toilet = house['layoutToilet']
            if not toilet:
                toilet = 0
            else:
                toilet = 0 if math.isnan(toilet) else toilet
            if room == -1:
                pattern = '開放式格局'
            elif (room==0 and hall==0 and toilet==0):
                pattern = ''
            else:
                pattern = f'{int(room)}房{int(hall)}廳{int(toilet)}衛'
            # 剩下進入物件頁面解析
            api_url = 'https://www.pacific.com.tw/api/ObjectAPI/GetObjectDetail/' + source_id
            s = requests.Session()
            time.sleep(random.uniform(5,11))
            for _ in range(3):
                try:
                    res = s.get(api_url, headers=header, proxies=proxy)
                    house_detail = res.json()
                    area = house_detail['areaName']
                    break
                except Exception as err:
                    print('物件連線失敗，重新連線中，請等待15～30秒')
                    time.sleep(random.randint(15,30))
            if _ == 2:
                self.logger.error(f"Error processing link {link}: {str(err)}")
                continue
            
            match = re.findall(f'{city}?{area}(.+)', address)
            road = match[0] if match else ''
            contact_type = '仲介'
            brand = house_detail['companyName']
            company = house_detail['storeCompany']
            branch = house_detail['storeName']
            contact_man = house_detail['saleEmployeeName']
            if not contact_man:
                contact_man = ''
            phone = house_detail['saleEmployeePhone']
            if not phone:
                phone = house_detail['storeTel'] # 若沒業務員電話，就填入分店電話
            coner = house_detail['hasInConer']
            if not coner:
                edge = ''
            elif coner == 1:
                edge = '是'
            elif coner == 2:
                edge = '否'
            light = house_detail['light']
            if not light:
                dark = ''
            elif light > 0:
                dark = '否'
            elif light == 2:
                dark = '是'
            is_feature = house_detail['objFeature1']
            if is_feature:
                feature = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9\s]+', '', is_feature)
                feature = feature.replace('<BR>', ' ')
            situation = house_detail['objPurposeName']
            parking_type = house_detail['stallTypeName']
            manage_fee = house_detail['adminFee']
            blockto = house_detail['directionName']
            if not situation:
                situation = ''
            if not parking_type:
                parking_type = ''
            if not manage_fee:
                manage_fee = ''
            else:
                manage_fee = str(manage_fee) + '元/月繳'
            if not blockto:
                blockto = ''
            if total_ping!= 0:
                price_ave = round(total/total_ping,2)
            else:
                if land_ping!=0:
                    price_ave = round(total/land_ping,2)
                else:
                    price_ave = 0

            # 此網站無提供以下資料
            pattern1, house_num, manage_type, community = '', '', '', ''
            
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