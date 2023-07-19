import requests
from bs4 import BeautifulSoup
import pandas as pd
import re,time
from fake_useragent import UserAgent
import random
from crawler.craw_common import *

class Era():
    def __init__(self):
        self.name = 'era'
        self.source = 'ERA'
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
        page = 0
        proxy = get_proxies()
        while True:
            page+=1
            url = f'https://www.erataiwan.com/buy?page={page}&s%5Bcities%5D={city}'
            header = {'User-Agent': self.user_agent.random }
            try:
                time.sleep(random.uniform(1,2))
                res = requests.get(url, headers=header, proxies=proxy)
                soup = BeautifulSoup(res.text, 'lxml')
                items = soup.find('div',{'class':'ev-list-objs'}).find_all('a')
                if not items:
                    print(f'City {city_code} 共{len(list_obj)}個物件')
                    break
                for item in items:
                    link = 'https://www.erataiwan.com' + item['href']
                    source_id = re.findall(r'/buy/(.+)', link)[0]
                    subject = item.find('div',{'class':'title'}).text
                    re_price = int(float(item.find('div',{'class':'e-price-view'}).text.strip().rstrip('萬')))
                    obj_data = {
                                "source_id" : source_id,
                                "subject" : subject,
                                "link" : link,
                                "city" : city,
                                "re_price" : int(re_price),
                                "insert_time" : time.strftime("%Y-%m-%d %H:%M:%S")
                                }
                    list_obj.append(obj_data)
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
        list_obj = is_new(self, city_code)
        data_list = []
        proxy = get_proxies()
        for obj in list_obj:
            try:
                subject = obj['subject']
                link = obj['link']
                source_id = obj['source_id']
                city = obj['city']
                total = obj['re_price']
                print(subject, link)
                header = {'User-Agent': self.user_agent.random }
                time.sleep(random.uniform(5,12))
                res = requests.get(link, headers=header, proxies=proxy)
                soup = BeautifulSoup(res.text, 'lxml')
                address = soup.find('p',{'class':'subtitle'}).text.strip().replace('台','臺')
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
                situation, house_type, manage_fee, blockto, parking_type = '', '', '', '', ''
                house_age, house_age_v, floor_web, floor, total_floor = '', 0, '', 0, 0
                info = soup.find('div',{"class":'info-wrapper content-section'})
                lis = info.find_all('li')
                for i in range(len(lis)):
                    if lis[i].find('span',{'class':'title'}).text == '用途':
                        situation = lis[i].find('span',{'class':'value'}).text
                    elif lis[i].find('span',{'class':'title'}).text == '型態':
                        house_type = lis[i].find('span',{'class':'value'}).text
                    elif lis[i].find('span',{'class':'title'}).text == '管理費':
                        manage_fee = lis[i].find('span',{'class':'value'}).text
                    elif lis[i].find('span',{'class':'title'}).text == '座向':
                        blockto = lis[i].find('span',{'class':'value'}).text
                    elif lis[i].find('span',{'class':'title'}).text == '車位':
                        parking_type = lis[i].find('span',{'class':'value'}).text
                    elif lis[i].find('span',{'class':'title'}).text == '屋齡':
                        house_age = lis[i].find('span',{'class':'value'}).text
                        match = re.search(r'(\d+)年', house_age)
                        year = int(match.group(1)) if match else 0
                        match = re.search(r'(\d+)個月', house_age)
                        month = int(match.group(1)) if match else 0
                        house_age_v = round((year + (month/12)),1)
                    elif lis[i].find('span',{'class':'title'}).text == '樓高/總樓層':
                        floors = lis[i].find('span',{'class':'value'}).text
                        if '/' in floors:
                            floor_web, total_floor = floors.split('/')
                        else:
                            floor_web = floors
                            total_floor = floors
                        total_floor = re.findall('\d+', total_floor)[0]
                        if 'B' in floor_web:
                            floor = -1
                        else:
                            match = re.findall('\d+',floor_web)
                            if match:
                                floor = int(match[0])
                spec = soup.find('div',{"class":'spec-wrapper content-section'})
                spec_lis = spec.find_all('li')
                total_ping, building_ping, public_ping, att1, att2 = 0,0,0,0,0
                for i in range(len(spec_lis)):
                    if spec_lis[i].find('span',{'class':'title'}).text == '登記總面積':
                        total_ping = float(spec_lis[i].find('span',{'class':'value'}).text.rstrip('坪'))
                    elif spec_lis[i].find('span',{'class':'title'}).text == '主建物':
                        building_ping = float(spec_lis[i].find('span',{'class':'value'}).text.rstrip('坪'))
                    elif spec_lis[i].find('span',{'class':'title'}).text == '公共':
                        public_ping = float(spec_lis[i].find('span',{'class':'value'}).text.rstrip('坪'))
                    elif spec_lis[i].find('span',{'class':'title'}).text == '平台/陽台':
                        att1 = float(spec_lis[i].find('span',{'class':'value'}).text.rstrip('坪'))
                    elif spec_lis[i].find('span',{'class':'title'}).text == '雨遮':
                        att2 = float(spec_lis[i].find('span',{'class':'value'}).text.rstrip('坪'))
                att_ping = att1 + att2
                feature  = ''
                for tag in soup.find('div',{'class':'obj-text-content'}):
                    # 內容分行
                    contents = tag.get_text(separator="\n").splitlines()
                    # 跌代每一行
                    for line in contents:
                        # 各別去判斷每一行的情況
                        if tag.get_text() != "":
                            feature += line
                            feature = feature.strip()
                            feature += "\n"
                feature = feature.strip()
                lat, lng = 0,0
                map = soup.select_one('#main-obj-map > div > iframe')
                if map:
                    match = re.findall(r"!3d(\d+.\d+)!", map['src'])
                    if match:
                        lat = round(float(match[0]),7)
                    match = re.findall(r"!2d(\d+.\d+)!", map['src'])
                    if match:
                        lng = round(float(match[0]),7)
                contact_man, phone, branch, company = '', '', '', ''
                agents = soup.find_all('div',{'class':'ev-agent-card'})
                for agent in agents:
                    man = agent.find('p',{'class':'title'})
                    if man and (man.text not in contact_man):
                        contact_man += (man.text + ';')
                    phonenum = agent.find('p',{'class':'phone'})
                    if phonenum and (phonenum.text not in phone):
                        phone += (phonenum.text + '; ')
                    era = agent.find('p',{'class':'era'})
                    if era and (era.text not in branch):
                        branch += (era.text + ';')
                    store = agent.find('p',{'class':'store'})
                    if store and (store.text not in company):
                        company += (store.text + ';')
                contact_man = contact_man.rstrip(';')
                phone = phone.rstrip('; ')
                branch = branch.rstrip(';')
                company = company.rstrip(';')
                brand = 'ERA'
                img = soup.find('div',{'class':'ev-cover-inbox-image'})
                if img:
                    img_url = 'https://www.erataiwan.com'+img.find('img')['src']
                else:
                    img_url = ''
                r_info = soup.find('ul',{'class':'section'})
                pings = r_info.find_all('li')[0].text
                try:
                    land_ping = float(re.findall('土地(坪數)* (\d+\.*\d*)坪', pings)[0][1])
                except:
                    land_ping = 0
                pattern = r_info.find_all('li')[1].text.strip()
                if total_ping!= 0:
                    price_ave = round(total/total_ping,2)
                else:
                    if land_ping!=0:
                        price_ave = round(total/land_ping,2)
                    else:
                        price_ave = 0
            
            except requests.exceptions.ProxyError as err:
                self.logger.error(f"Proxy Error processing link {link}: {str(err)}")
                time.sleep(random.uniform(5,10))
                proxy = get_proxies()
                continue

            except Exception as e:
                self.logger.error(f"Error processing link {link}: {str(e)}")
                continue

            # 此網站無提供以下資訊
            pattern1, house_num, manage_type, edge, dark, contact_type, community = '', '', '', '', '', '', ''
            
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