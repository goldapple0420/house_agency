import requests
from bs4 import BeautifulSoup
import re
import time
import pandas as pd
import random
from fake_useragent import UserAgent
from crawler.craw_common import *

class Myhomes():
    def __init__(self):
        self.name = 'myhomes'
        self.source = '我家網'
        self.user_agent = UserAgent()
        self.logger = WriteLogger().getLoggers()

    def get_items(self, city_code):
        # 金門跟連江放在一起
        all_city_dict = {
        "A": {"city_c": "臺北市", "city_n": "0"},
        "B": {"city_c": "臺中市", "city_n": "8"},
        "C": {"city_c": "基隆市", "city_n": "1"},
        "D": {"city_c": "臺南市", "city_n": "14"},
        "E": {"city_c": "高雄市", "city_n": "15"},
        "F": {"city_c": "新北市", "city_n": "2"},
        "G": {"city_c": "宜蘭縣", "city_n": "3"},
        "H": {"city_c": "桃園市", "city_n": "4"},
        "I": {"city_c": "嘉義市", "city_n": "12"},
        "J": {"city_c": "新竹縣", "city_n": "6"},
        "K": {"city_c": "苗栗縣", "city_n": "7"},
        "M": {"city_c": "南投縣", "city_n": "10"},
        "N": {"city_c": "彰化縣", "city_n": "9"},
        "O": {"city_c": "新竹市", "city_n": "5"},
        "P": {"city_c": "雲林縣", "city_n": "11"},
        "Q": {"city_c": "嘉義縣", "city_n": "13"},
        "T": {"city_c": "屏東縣", "city_n": "17"},
        "U": {"city_c": "花蓮縣", "city_n": "19"},
        "V": {"city_c": "臺東縣", "city_n": "18"},
        "X": {"city_c": "澎湖縣", "city_n": "16"},
        "W": {"city_c": "金門連江", "city_n": "20"},
        "Z": {"city_c": "金門連江", "city_n": "20"}
        }
        list_obj = []
        city_n = all_city_dict[city_code]['city_n']
        city = all_city_dict[city_code]['city_c']
        proxy = get_proxies()
        page = 0
        while True:
            page+=1
            url = f'https://www.myhomes.com.tw/objects/index/bda/2/{page}?s[city]={city_n}' # 依刊登日期bda排序
            header = {"User-Agent": self.user_agent.random}
            try:
                time.sleep(random.uniform(2,5))
                res = requests.get(url, headers=header, proxies=proxy)
                print(f'開始擷取{self.source} {city} P{page} 物件')
                soup = BeautifulSoup(res.text, 'lxml')
                items = soup.find_all('div',{'style':'border-bottom:0px;'})
                if not items:
                    break
                for item in items:
                    link = item.find('a')['href']
                    source_id = re.findall('objects/o/(\d+)', link)[0]
                    subject = item.find('h3').find_all('a')[-1].get_text(strip=True)
                    if '已售出' in subject:
                        continue
                    re_price = float(item.find('span',{'class':'red-b'}).text.rstrip(' 萬'))
                    # 金門連江要找出是金門還是連江
                    if city == '金門連江':
                        address = item.find('ul').find_all('li')[1].get_text(strip=True)
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
            
            except requests.exceptions.ProxyError as err:
                self.logger.error(f"Proxy Error processing link {link}: {str(err)}")
                time.sleep(random.uniform(5,10))
                proxy = get_proxies()

            except requests.exceptions.HTTPError as err:
                self.logger.error(f"HTTPError processing link {link}: {str(err)}")
                time.sleep(random.uniform(10,15))
                proxy = get_proxies()
            
            except Exception as e:
                self.logger.error(f"Error processing City{city} Page{page}: {str(e)}")
        
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
        header = {"User-Agent":self.user_agent.random}
        proxy = get_proxies()
        for obj in list_obj:
            subject = obj['subject']
            if '已售出' in subject:
                continue
            link = obj['link']
            source_id = obj['source_id']
            city = obj['city']
            total = obj['re_price']
            print(subject, link)
            time.sleep(random.uniform(2,5))
            try:
                res = requests.get(link, headers=header, proxies=proxy)
                soup = BeautifulSoup(res.text, 'lxml')
                address = soup.select_one('#detail-addr')
                area = address.find_all('a')[1].text.strip()
                # 有可能會沒寫路名（例如土地）
                try:
                    road = re.findall(f'{city}\s+{area}\s+(.+)\s+地圖', address.text)[0].strip()
                except:
                    road = ''
                address = city + area + road
                total_ping, building_ping, att_ping, public_ping, land_ping = 0,0,0,0,0
                floor_web, floor, total_floor, house_age, house_age_v = '',0,0,'',0
                situation, house_type, community, pattern, parking_type = '', '', '', '', ''
                right_info = soup.select_one('#main > div.left-column > div.main-info > div.house-item > ul').text
                match = re.findall('總坪數：(\d+\.*\d*) 坪', right_info)
                if match:
                    total_ping = float(match[0])
                match = re.findall('主建物面積：(\d+\.*\d*) 坪', right_info)
                if match:
                    building_ping = float(match[0])
                match = re.findall('附屬建物面積：(\d+\.*\d*) 坪', right_info)
                if match:
                    att_ping = float(match[0])
                match = re.findall('土地面積：(\d+\.*\d*) 坪', right_info)
                if match:
                    land_ping = float(match[0])
                match = re.findall('共同使用：(\d+\.*\d*) 坪', right_info)
                if match:
                    public_ping = float(match[0])
                match = re.findall('屋齡：(\d+\.*\d*) 年', right_info)
                if match:
                    house_age = match[0] + '年'
                    house_age_v = float(match[0])
                match = re.findall('參考單價：(\d+\.*\d*) 萬/坪', right_info)
                if match:
                    price_ave = float(match[0])
                match = re.findall('法定用途：(.+)\n', right_info)
                if match:
                    situation = match[0]
                match = re.findall('形式/類型：.+/\s+(.+)\n', right_info)
                if match:
                    house_type = match[0]
                match = re.search('樓別/樓高：\s+(\w*\~*\d+樓)\s+/ (\d+)樓', right_info)
                if match:
                    floor_web = match.group(1)
                    total_floor = int(match.group(2))
                    if 'B' in floor_web:
                        floor = -1
                    else:
                        floor = re.findall('\d+', floor_web)[0]
                match = re.findall('社區：(.+)', right_info)
                if match:
                    community = match[0]
                match = re.findall('格局：\s+(.+)\s+', right_info)
                if match:
                    pattern = match[0].replace(' ','')
                match = re.findall('車位：(.+)', right_info)
                if match:
                    parking_type = match[0].replace(' ','')
                manage_type, manage_fee, blockto, feature, img_url, lat, lng = '', '', '', '', '',0,0
                detail = soup.find('div',{'class':'coursr-content'}).text
                match = re.findall('管理方式：(.+)', detail)
                if match:
                    manage_type = match[0]
                match = re.findall('管理費：(.+)', detail)
                if match:
                    manage_fee = match[0]
                match = re.findall('朝向方位：(.+)', detail)
                if match:
                    blockto = match[0]
                is_feature = soup.find('meta',{'name':'description'})
                if is_feature:
                    feature = is_feature['content']
                img = soup.select_one('#myimg1')
                if img:
                    img_url = img.find('img')['src']
                map = soup.select_one('#wrapper > script:nth-child(8)')
                if map:
                    match = re.search('lat: (\d+.\d+) ,\s+lng: (\d+.\d+)', map.text)
                    if match:
                        lat = match.group(1)
                        lng = match.group(2)
                # 土地的物件頁面上沒寫類型house_type
                if ('土地' in situation) and (house_type == ''):
                    situation, house_type = '土地','土地'
                
                contact_man, phone, company, branch, brand, contact_type = '', '', '', '', '', ''
                contact_type = '仲介'
                # 物件賣家資料分兩區塊
                if soup.find('div',{'class':'agency'}):
                    contact_man = soup.find_all('span',{'class':'agency-name'})[1].text
                    phone = soup.find('span',{'class':'phone'}).text
                    is_company = soup.select_one('#main > div.right-column > div.agency > p:nth-child(11)')
                    if is_company:
                        match = re.findall('經紀業名稱: (.+)', is_company.text)
                        if match:
                            company = match[0]
                    shop = soup.find('p',{'style':'color:#1b1b67;'})
                    if shop:
                        try: # 少部份資料未填寫
                            branch = re.findall(' (.+ [加盟店|直營店])', shop.text)[0]
                            # 有些沒寫brand(ex. XX房屋 XX不動產...)
                            match = re.findall(f'(.+) {branch}', shop.text)
                            if match:
                                brand = match[0]
                        except:
                            pass
                # <!-- 不是社群房仲, 也不是菁英店家, 也不是金牌業務員 -->
                elif soup.find('div',{'class':'agency-card'}):
                    print('非社群房仲')
                    contact_man = soup.find('div',{'class':'name'}).text
                    is_company = soup.select_one('#main > div.left-column > div.agency-card > ul > li:nth-child(4)')
                    if is_company:
                        match = re.findall('經紀業名稱: (.+)', is_company.text)
                        if match:
                            company = match[0]
                    shop = soup.select_one('#main > div.left-column > div.agency-card > ul > li:nth-child(3)')
                    if shop:
                        branch = re.findall(' (.+ [加盟店|直營店])', shop.text)[0]
                        match = re.findall(f'(.+) {branch}', shop.text)
                        if match:
                            brand = match[0]
            
            except requests.exceptions.ProxyError as err:
                self.logger.error(f"Proxy Error processing link {link}: {str(err)}")
                time.sleep(random.uniform(5,10))
                proxy = get_proxies()
                continue

            except Exception as e:
                self.logger.error(f"Error processing link {link}: {str(e)}")
                continue
            
            # 此網站無提供以下資訊
            pattern1, house_num, edge, dark = '', '', '', ''
            
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