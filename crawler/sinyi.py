import requests
from bs4 import BeautifulSoup
import pandas as pd
import re, time
from fake_useragent import UserAgent
import random
from crawler.craw_common import  *

class Sinyi():
    def __init__(self):
        self.name = 'sinyi'
        self.source = '信義房屋'
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
        city = all_city_dict[city_code]
        house_list = []
        proxy = get_proxies()
        page = 0
        while True:
            page+=1
            url = 'https://sinyiwebapi.sinyi.com.tw/searchObject.php'
            header = {'User-Agent': self.user_agent.random,
                    'code':'0',
                    'sat':'730282',
                    'sid':'20230530113006065',
                    'Content-Type': 'application/json;charset=UTF-8',
                    'X-Powered-By': 'PHP/5.6.40'}
            time.sleep(random.uniform(2,6))
            ip = proxy['https']
            data = '{"machineNo":"","ipAddress":'+f'"{ip}"'+',"osType":5,"model":"web","deviceVersion":"Linux","appVersion":"111.0.0.0","deviceType":3,"apType":3,"browser":1,"memberId":"","domain":"www.sinyi.com.tw","utmSource":"","utmMedium":"","utmCampaign":"","utmCode":"","requestor":1,"utmContent":"","utmTerm":"","sinyiGroup":1,"filter":{"exludeSameTrade":false,"objectStatus":0,"retType":1,"retRange":["11"]},"page":'+str(page)+',"pageCnt":20,"sort":"2","isReturnTotal":true}'
            s = requests.Session()
            res = s.post(url, data=data, headers=header, proxies=proxy)
            print(f'開始擷取{self.source} {city} P{page} 物件')
            page_list = res.json()['content']['object']
            if not page_list:
                break
            for house in page_list:
                house['source_id'] = house.pop('houseNo')
                house['subject'] = house.pop('name')
                house['link'] = house.pop('shareURL')
                house['re_price'] = house.pop('totalPrice')
                house['city'] = city
                del house['image']
                del house['houselandtypeShow']
                del house['tags']
                # 有些沒停車的就是null
                try:
                    house['parking'] = house['parking'][0]
                except:
                    pass
                try:
                    house['houselandtype'] = house['houselandtype'][0]
                except:
                    pass
                house_list.append(house)
        
        if house_list:
            to_sqlite(self, house_list)
        else: # 是空的也要建立空資料表
            con = sqlite3.connect(os.path.abspath('compare.db'))
            table_name = f'{self.name}_{time.strftime("%m%d")}'
            sql = f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                "latitude"	REAL,
                "longitude"	REAL,
                "largeImage"	TEXT,
                "imageTag"	TEXT,
                "commId"	TEXT,
                "commName"	TEXT,
                "discount"	INTEGER,
                "address"	TEXT,
                "age"	TEXT,
                "houselandtype"	TEXT,
                "priceFirst"	INTEGER,
                "areaBuilding"	REAL,
                "areaLand"	REAL,
                "isHasBalcony"	INTEGER,
                "pingUsed"	REAL,
                "layout"	TEXT,
                "floor"	TEXT,
                "totalfloor"	TEXT,
                "isParking"	INTEGER,
                "parking"	TEXT,
                "threeMonthsClicks"	INTEGER,
                "isOff"	INTEGER,
                "isSimilar"	INTEGER,
                "status"	INTEGER,
                "kind"	INTEGER,
                "uniPrice"	TEXT,
                "objectType"	INTEGER,
                "3DVR"	INTEGER,
                "zipCode"	TEXT,
                "groupCompany"	TEXT,
                "addLayout"	TEXT,
                "totalLayout"	TEXT,
                "isHasVideo"	INTEGER,
                "isHasView"	INTEGER,
                "source_id"	TEXT,
                "subject"	TEXT,
                "link"	TEXT,
                "re_price"	INTEGER,
                "city"	TEXT
                );
            '''
            con.execute(sql)
            con.close()


    def get_data(self, city_code):
        data_list = []
        list_obj = is_new(self, city_code)
        for obj in list_obj:
            try:
                source_id = obj['source_id']
                link = obj['link']
                subject = obj['subject']
                total = obj['re_price']
                city = obj['city']
                print(subject, link)
                lat = round(obj['latitude'],7)
                lng = round(obj['longitude'],7)
                img_url = obj['largeImage']
                address = obj['address'].replace('台','臺')
                land_ping = obj['areaLand']
                total_ping = obj['areaBuilding']
                pattern = obj['layout']
                pattern1 = obj['addLayout']
                floor_web = obj['floor']
                total_floor = obj['totalfloor']
                house_age = obj['age']
                community = obj['commName']
                # 其他資訊須到物件頁面
                if len(data_list) % 10 == 0:
                    proxy = get_proxies()
                ip = proxy['https']
                data = '{"machineNo":"","ipAddress":'+f'"{ip}"'+',"osType":5,"model":"web","deviceVersion":"Linux","appVersion":"111.0.0.0","deviceType":3,"apType":3,"browser":1,"memberId":"","domain":"www.sinyi.com.tw","utmSource":"","utmMedium":"","utmCampaign":"","utmCode":"","requestor":1,"utmContent":"","utmTerm":"","sinyiGroup":1,"houseNo":'+ f'"{source_id}"' +',"agentId":"","memberPhone":"","showOff":0}'
                header = { 'User-Agent' : self.user_agent.random ,
                        'sat': '730282',
                        'sid': '20230530113006065',
                        'code': '0'
                        }
                s = requests.Session()
                # 紀錄物件基本資料的api
                api_url = 'https://sinyiwebapi.sinyi.com.tw/getObjectContent.php'
                time.sleep(random.uniform(7,18))
                res = s.post(api_url, headers=header, timeout=(5,30), data=data, proxies=proxy)
                res.raise_for_status()
                obj_content = res.json()['content']
                area = obj_content['zipName']
                contact_man = obj_content['agent']['agentName']
                store = obj_content['agent']['agentStore']
                phone = obj_content['agent']['agentTel']
                manage_type = obj_content['hasmanager']
                manage_fee = obj_content['monthlyFee']
                parking_type = obj_content['parking']
                is_dark = obj_content['sfdarkroom']
                is_side = obj_content['sfside']
                blockto = obj_content['buildingFront']
                # toiletplus = obj_content['bathroomplus']
                # hallplus = obj_content['hallplus']
                # roomplus = obj_content['roomplus']
                # openroomplus = obj_content['openroomplus']
                houselandtype = obj_content['houselandtype']
                pings = obj_content['areaInfo']
                agent_id = obj_content['agent']['agentId']
                
                # 第二隻api紀錄謄本資訊跟特色等其他詳細資料
                api_url2 = 'https://sinyiwebapi.sinyi.com.tw/getObjectDetail.php'
                time.sleep(random.uniform(6,12))
                res2 = s.post(api_url2, headers=header, timeout=(5,30), data=data, proxies=proxy)
                obj_detail = res2.json()['content']
                situation = obj_detail['detail']['purpose']
                house_num = obj_detail['detail']['family']
                feature = ','.join(obj_detail['description'])
                
                # 將抓下來的資料做清理
                if not situation:
                    situation = ''
                if not house_num:
                    house_num = ''
                if not blockto:
                    blockto = ''
                if not community:
                    community = ''
                if not parking_type:
                    parking_type = ''
                # 屋齡
                if house_age == '--' or house_age == '預售':
                    house_age_v = 0
                else:
                    house_age_v = float(house_age.rstrip('年'))
                # 管理費
                if not manage_type:
                    manage_type = ''
                if not manage_fee:
                    manage_fee = ''
                else:
                    manage_fee = manage_fee + '元/月'
                # 房仲
                contact_type = '仲介'
                if '(' in store:
                    branch, company = store.split('(')
                    company = company.rstrip(')')
                else:
                    branch = store
                    company = ''
                if agent_id:
                    brand = '信義房屋'
                else:# 非信義房屋的物件，brand要到物件網頁找
                    obj_header = {'User-Agent': self.user_agent.random}
                    time.sleep(random.uniform(3,8))
                    obj_res = s.get(link, headers=obj_header, proxies=proxy)
                    obj_soup = BeautifulSoup(obj_res.text, 'lxml')
                    is_brand = obj_soup.find('div',{'class':'buy-content-store-title'})
                    brand = is_brand.text.split('-')[0] if is_brand else ''
                # 邊間/暗房
                if is_dark == 'true':
                    dark = '是'
                else:
                    dark = '否'
                if is_side == 'true':
                    edge = '是'
                else:
                    edge = '否'
                # 格局
                if not pattern1:
                    pattern1 = ''
                if not pattern:
                    pattern = ''
                # pattern1 = ''
                # if roomplus >0:
                #     pattern1 += f'{roomplus}房'
                # if hallplus >0:
                #     pattern1 += f'{hallplus}廳'
                # if toiletplus >0:
                #     pattern1 += f'{toiletplus}衛'
                # if openroomplus >0:
                #     pattern1 += f'{openroomplus}室'
                # 房屋類型
                if not houselandtype:
                    houselandtype = ''
                else:
                    houselandtype = houselandtype[0]
                if houselandtype == 'A':
                    house_type = '公寓'
                elif houselandtype == 'C':
                    house_type = '套房'
                elif houselandtype == 'L':
                    house_type = '大樓'
                elif houselandtype == 'M':
                    house_type = '華廈'
                elif houselandtype == 'D':
                    house_type = '透天'
                elif houselandtype == 'E':
                    house_type = '店面'
                elif houselandtype == 'I':
                    house_type = '土地'
                elif houselandtype == 'F':
                    house_type = '辦公'
                elif houselandtype == 'G':
                    house_type = '廠房'
                elif houselandtype == 'H':
                    house_type = '倉庫'
                elif houselandtype == 'J':
                    house_type = '單售車位'
                elif houselandtype == 'K':
                    house_type = '其他'
                else:
                    house_type = ''
                # 路名
                is_road = re.findall(f'{city}{area}(.+)', address)
                road = is_road[0] if is_road else ''
                # 單價
                if total_ping!= 0:
                    price_ave = round(total/total_ping,2)
                else:
                    if land_ping!=0:
                        price_ave = round(total/land_ping,2)
                    else:
                        price_ave = 0
                # 坪數
                building_ping, att_ping, public_ping = 0,0,0
                for ping in pings:
                    if ping['title'] == '主建物':
                        building_ping += ping['area']
                    elif ping['title'] == '共有部份':
                        public_ping += ping['area']
                    else:
                        if ping['title'] != '主+陽':
                            att_ping += ping['area']
                # 樓層
                if 'B' in floor_web:
                    floor = -1
                else:
                    match1 = re.findall('(^-?\d+)-?\d?', floor_web) # -4 or -1-3 or 1-3
                    if match1:
                        floor = int(match1[0])
                    else:
                        floor = 0
            
            except requests.exceptions.ProxyError as err:
                self.logger.error(f"Proxy Error processing link {link}: {str(err)}")
                time.sleep(random.uniform(15,30))
                proxy = get_proxies()
            
            except requests.exceptions.HTTPError as err:
                self.logger.error(f"HTTPError processing link {link}: {str(err)}")
                time.sleep(random.uniform(8,20))
                proxy = get_proxies()
                continue

            except Exception as err:
                self.logger.error(f"Error processing link {link}: {str(err)}")
                continue
            
            else:
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