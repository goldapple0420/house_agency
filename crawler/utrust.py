from bs4 import BeautifulSoup
import pandas as pd
import re,time
from fake_useragent import UserAgent
import random
import urllib.request
import ssl
from crawler.craw_common import  *
import http.client

# 創建一個不驗證證書的上下文
context = ssl.create_default_context()
context.check_hostname = False
context.verify_mode = ssl.CERT_NONE

class MyHTTPSHandler(urllib.request.HTTPSHandler):
    def https_open(self, req):
        return self.do_open(http.client.HTTPSConnection, req, context=context)

class Utrust():
    def __init__(self):
        self.name = 'utrust'
        self.source = '有巢氏房屋'
        self.user_agent = UserAgent()
        self.logger = WriteLogger().getLoggers()


    def get_items(self, city_code):
        all_city_dict = {
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
        list_obj = []
        city = all_city_dict[city_code]
        city_n = city.replace('臺','台')
        url = 'https://www.u-trust.com.tw/CaseBuy/Ashx/ShowList2.ashx'
        headers = {'User-Agent' : self.user_agent.random}
        proxy = get_proxies()
        proxy_handler = urllib.request.ProxyHandler(proxy)
        opener = urllib.request.build_opener(MyHTTPSHandler())
        opener.add_handler(proxy_handler)
        page=0
        while True:
            page+=1
            data = f'County={city_n}&ListMode=PhotosAndWords&PageCount=40&CurrentPage={page}&SearchMode=1'
            try:
                req = urllib.request.Request(url, data.encode("utf-8"), headers)
                with opener.open(req) as f:
                    res = f.read()
                html = res.decode()
                print(f'開始擷取{self.source} {city} P{page} 物件')
                soup = BeautifulSoup(html, 'lxml')
                items = soup.find_all('div',{'class':'house_block'})
                if not items:
                    break
                for item in items:
                    id = re.findall('CItem_(\d+)', item['id'])[0]
                    if id != '0': # id等於0的物件是連結到台慶不動產網站
                        source_id = id
                        link = 'https://www.u-trust.com.tw/CaseBuy/House/' + source_id + '/'
                        subject = item.find('h2').find('a')['title'].strip()
                        re_price = float(item.find('p',{'class':'price'}).text.replace(',','').rstrip('萬'))
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
                time.sleep(random.uniform(10,20))
                proxy = get_proxies()
            
            except Exception as e:
                self.logger.error(f"Error processing City {city_code} Page {page}: {str(e)}")
                break # 測試才放（有錯就跳開
        
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
        proxy = get_proxies()
        proxy_handler = urllib.request.ProxyHandler(proxy)
        opener = urllib.request.build_opener(MyHTTPSHandler())
        opener.add_handler(proxy_handler)
        for obj in list_obj:
            try:
                subject = obj['subject']
                link = obj['link']
                source_id = obj['source_id']
                city = obj['city']
                total = obj['re_price']
                print(subject, link)
                headers = {'User-Agent' : self.user_agent.random}
                time.sleep(random.uniform(2,5))
                req = urllib.request.Request(link, headers=headers)
                with opener.open(req) as f:
                    res = f.read() 
                html = res.decode()
                soup = BeautifulSoup(html, 'lxml')
                # 物件若是售出，點進去後會將頁面轉到買屋首頁
                subject = soup.select_one('#wrapper > h1').text
                if '中古屋搜尋－有巢氏房屋' in subject: 
                    print('此物件已不存在')
                    continue
                situation, address = soup.select_one('#wrapper > h2').text.split(',')
                area = soup.select_one('#breadCrumb > li:nth-child(7) > a').text
                is_road = soup.select_one('#breadCrumb > li:nth-child(9) > a') # 網站可能沒提供路名
                road = is_road.text if is_road else ''
                phone = soup.find('span',{'class':'DTel'}).text
                total_ping, land_ping, building_ping = 0,0,0
                house_age, house_age_v, floor_web, floor, total_floor = '',0,'',0,0
                detail_main = soup.select_one('#ctl00_ContentPlaceHolder1_detail_main_txt').text
                # print(detail_main)
                match = re.findall('建物格局：(.+)', detail_main)
                if match:
                    pattern = match[0]
                match = re.findall('建物坪數：(\d+\.*\d*)坪', detail_main)
                if match:
                    total_ping = float(match[0])
                match = re.findall('土地登記：(\d+\.*\d*)坪', detail_main)
                if match:
                    land_ping = float(match[0])
                match = re.findall('主建物[^：]*：(\d+\.*\d*)坪', detail_main)
                if match:
                    building_ping = float(match[0])
                match = re.findall('屋齡：(\d+\.*\d*)年', detail_main)
                if match:
                    house_age = match[0] + '年'
                    house_age_v = float(match[0])
                match = re.findall('樓別/樓高：(\w+ ~ \d+ / \d+)', detail_main)
                if match:
                    floor_web, total_floor = match[0].split(' / ')
                    total_floor = int(total_floor)
                    if 'B' in floor_web:
                        floor = -1
                    else:
                        floor = int(floor_web.split(' ~ ')[0])
                house_type, manage_fee, edge, parking_type, manage_type, blockto = '', '', '', '', '', ''
                detail = soup.select_one('#menu > div:nth-child(6) > div > table').text
                match = re.findall('型態：(.+)', detail)
                if match:
                    house_type = match[0]
                match = re.findall('建物管理費：(.+元)', detail)
                if match:
                    manage_fee = match[0]
                match = re.findall('是否邊間：([是｜否])', detail)
                if match:
                    edge = match[0]
                match1 = re.findall('停車方式：(.+)', detail)
                match2 = re.findall('車位狀況：(.+)', detail)
                if match1:
                    parking1 = match1[0]
                    if parking1 == '--':
                        parking1 = ''
                if match2:
                    parking2 = match2[0]
                    if parking2 == '--':
                        parking2 = ''
                parking_type = parking1 + parking2
                match = re.findall('管理方式：(.+)', detail)
                if match:
                    manage_type = match[0]
                match = re.findall('朝向：(.+)', detail)
                if match:
                    blockto = match[0]
                feature = soup.select_one('#menu > div:nth-child(6) > div > div:nth-child(3)').text.strip().lstrip('房屋描述').strip()
                img = soup.select_one('#ctl00_ContentPlaceHolder1_Pic1')
                if img:
                    img_url = img['src']
                else:
                    img_url = ''
                shop = soup.select_one('#wrapper > div.detail_main.clearfix > div.detail_main_right > div:nth-child(1) > h3 > b > a:nth-child(2)')
                branch, company = '', ''
                if shop:
                    if '-' in shop.text:
                        branch, company = shop.text.split('-')
                contact_type = '仲介'
                brand = '有巢氏房屋'
                lat, lng = 0,0
                locate = soup.select_one('#breadCrumb > li:nth-child(5) > a')['onclick']
                match = re.search(f"'{city}','','(\d+\.\d+)','(\d+\.\d+)'", locate)
                if match:
                    lat = float(match.group(2))
                    lng = float(match.group(1))
                if total_ping!= 0:
                    price_ave = round(total/total_ping,2)
                else:
                    if land_ping!=0:
                        price_ave = round(total/land_ping,2)
                    else:
                        price_ave = 0
            
            except requests.exceptions.ProxyError as err:
                self.logger.error(f"Proxy Error processing link {link}: {str(err)}")
                time.sleep(random.uniform(15,30))
                proxy = get_proxies()
                continue

            except Exception as e:
                self.logger.error(f"Error processing link {link}: {str(e)}")
                continue
            
            # 此網站無提供以下資訊
            pattern1, house_num, dark, contact_man, community = '', '', '', '', ''
            public_ping, att_ping = 0,0
            
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