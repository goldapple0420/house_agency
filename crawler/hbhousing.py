import requests
from bs4 import BeautifulSoup
import pandas as pd
import re,time,json
from fake_useragent import UserAgent
import random
from crawler.craw_common import *

class HBhouse():
    def __init__(self):
        self.name = 'hbhousing'
        self.source = '住商'
        self.user_agent = UserAgent()
        self.logger = WriteLogger().getLoggers()
        self.all_city_dict = {
        "A": {"city_c": "臺北市", "city_n": "3"},
        "B": {"city_c": "臺中市", "city_n": "9"},
        "C": {"city_c": "基隆市", "city_n": "2"},
        "D": {"city_c": "臺南市", "city_n": "16"},
        "E": {"city_c": "高雄市", "city_n": "18"},
        "F": {"city_c": "新北市", "city_n": "4"},
        "G": {"city_c": "宜蘭縣", "city_n": "1"},
        "H": {"city_c": "桃園市", "city_n": "5"},
        "I": {"city_c": "嘉義市", "city_n": "14"},
        "J": {"city_c": "新竹縣", "city_n": "7"},
        "K": {"city_c": "苗栗縣", "city_n": "8"},
        "M": {"city_c": "南投縣", "city_n": "11"},
        "N": {"city_c": "彰化縣", "city_n": "12"},
        "O": {"city_c": "新竹市", "city_n": "6"},
        "P": {"city_c": "雲林縣", "city_n": "13"},
        "Q": {"city_c": "嘉義縣", "city_n": "15"},
        "T": {"city_c": "屏東縣", "city_n": "20"},
        "U": {"city_c": "花蓮縣", "city_n": "22"},
        "V": {"city_c": "臺東縣", "city_n": "21"},
        "X": {"city_c": "澎湖縣", "city_n": "23"},
        "W": {"city_c": "金門縣", "city_n": "24"},
        "Z": {"city_c": "連江縣", "city_n": "25"}
        }

    def get_items(self, city_code):
        city = self.all_city_dict[city_code]['city_c']
        city_n = self.all_city_dict[city_code]['city_n']
        index_url = 'https://www.hbhousing.com.tw/ajax/dataService.aspx?job=search&path=house'
        header = {
            'Origin': 'https://www.hbhousing.com.tw',
            'Referer': 'https://www.hbhousing.com.tw/BuyHouse/',
            'User-Agent': self.user_agent.random,
            'X-Requested-With': 'XMLHttpRequest'
        }
        proxy = get_proxies()
        house_list = []
        page, last_page = 0,1
        while page<last_page:
            page+=1
            data = {'q': f'2^1^{city_n}^^^P^^^^^^^^^^^^^^^{page}^0', 'rlg': '0'}
            try:
                s = requests.session()
                time.sleep(random.uniform(1,5))
                insex_res = s.post(index_url, data=data, headers=header, proxies=proxy)
                page_list = insex_res.json()['data']
                last_page = insex_res.json()['a']
                for house in page_list:
                    house['i'] = house['i'][0]
                    del house['p']
                    house['source_id'] = house.pop('s')
                    house['re_price'] = int(house.pop('np'))
                    house['subject'] = house.pop('n')
                    house['city'] = city
                    house['source'] = self.source
                    house_list.append(house)
            
            except requests.exceptions.ProxyError as err:
                self.logger.error(f"Proxy Error processing page{page}: {str(err)}")
                time.sleep(random.uniform(5,10))
                proxy = get_proxies()

            except Exception as e:
                self.logger.error(f"Error processing City {city_code} Page {page}: {str(e)}")
        
        if house_list:
            to_sqlite(self, house_list)
        else: # 是空的也要建立空資料表
            con = sqlite3.connect(os.path.abspath('compare.db'))
            table_name = f'{self.name}_{time.strftime("%m%d")}'
            sql = f'''
            CREATE TABLE IF NOT EXISTS {table_name}(
                "g"	TEXT,
                "mrt"	TEXT,
                "f"	TEXT,
                "i"	TEXT,
                "is"	TEXT,
                "pa"	TEXT,
                "pp"	TEXT,
                "a"	REAL,
                "u"	TEXT,
                "x"	TEXT,
                "t"	TEXT,
                "y"	TEXT,
                "w"	TEXT,
                "z"	TEXT,
                "k"	TEXT,
                "ti"	TEXT,
                "c"	TEXT,
                "source_id"	TEXT,
                "re_price"	INTEGER,
                "subject"	TEXT,
                "city"	TEXT,
                "source"	TEXT
            );
            '''
            con.execute(sql)
            con.close()

    def get_data(self, city_code):
        house_list = is_new(self, city_code)
        data_list = []
        for house in house_list:
            if len(data_list) % 10 == 0:
                proxy = get_proxies()
            source_id = house['source_id']
            subject = house['subject']
            link = 'https://www.hbhousing.com.tw/detail/?sn=' + source_id
            print(subject, link)
            total = house['re_price']
            total_ping = house['a']
            img_url = 'https:' + house['i']
            house_age_v = house['k']
            if house_age_v:
                house_age = str(house_age_v) + '年'
            else:
                house_age_v, house_age = 0,''
            house_type = house['t']
            address = house['x'].replace('台','臺')
            floor_web = house['w']
            feature = house['g']
            if 'B' in floor_web:
                floor = -1
            else:
                match = re.findall('\d+', floor_web)
                if match:
                    floor = int(match[0])
                else:
                    floor = 0
            total_floors = house['z']
            total_floor = int(total_floors) if total_floors else 0
            
            # 以下資訊需進入物件網頁擷取
            header = {'User-Agent': self.user_agent.random }
            time.sleep(random.randint(3,10))
            try:
                res = requests.get(link, headers=header, proxies=proxy)
                soup = BeautifulSoup(res.text, 'lxml')
                add = soup.find('div',{'class':'breadcrumb'})
                span = add.find_all('span')
                city = span[2].text.replace('台','臺')
                area = span[3].text
                road = span[4].text
                building_ping, att_ping, public_ping, land_ping = 0,0,0,0
                situation, blockto, manage_fee, manage_type, parking_type, community = '', '', '', '', '', ''
                deatail = soup.find('div',{'class':'basicinfo-box'}).find_all('tr')
                for i in range(len(deatail)):
                    if deatail[i].find_all('td')[0].text == '主建物':
                        match = re.findall('(\d+\.*\d*)坪',deatail[i].find_all('td')[1].text)
                        if match:
                            building_ping = float(match[0])
                    elif deatail[i].find_all('td')[0].text == '附屬建物':
                        match = re.findall('(\d+\.*\d*)坪',deatail[i].find_all('td')[1].text)
                        if match:
                            att_ping = float(match[0])
                    elif deatail[i].find_all('td')[0].text == '公共設施':
                        match = re.findall('(\d+\.*\d*)坪',deatail[i].find_all('td')[1].text)
                        if match:
                            public_ping = float(match[0])
                    elif deatail[i].find_all('td')[0].text == '土地坪數':
                        match = re.findall('(\d+\.*\d*)坪',deatail[i].find_all('td')[1].text)
                        if match:
                            land_ping = float(match[0])
                    elif deatail[i].find_all('td')[0].text == '地坪':
                        match = re.findall('(\d+\.*\d*)坪',deatail[i].find_all('td')[1].text)
                        if match:
                            land_ping = float(match[0])
                    elif deatail[i].find_all('td')[0].text == '法定用途':
                        situation = deatail[i].find_all('td')[1].text
                    elif deatail[i].find_all('td')[0].text == '朝向':
                        blockto = deatail[i].find_all('td')[1].text.replace(' ','').replace('同朝向','')
                        if blockto == '座朝':
                            blockto = ''
                    elif '管理費' in deatail[i].find_all('td')[0].text :
                        manage_fee = deatail[i].find_all('td')[1].text.replace(' ','')
                        if manage_fee == '--':
                            manage_fee = ''
                    elif deatail[i].find_all('td')[0].text == '管理':
                        manage_type = deatail[i].find_all('td')[1].text
                    elif deatail[i].find_all('td')[0].text == '車位':
                        parking_type = deatail[i].find_all('td')[1].text.strip()
                    elif deatail[i].find_all('td')[0].text == '社區':
                        community = deatail[i].find_all('td')[1].text
                        if community == '--':
                            community = ''
                img = soup.select_one('#MainContent_objImgList')
                if img:
                    img_url = 'https:' + img.find('img')['rel']
                else:
                    img_url = ''
                
                # 經緯度
                map_url = f'https://www.hbhousing.com.tw/detail/map.aspx'
                map_header = {
                    'Host': 'www.great-home.com.tw',
                    'Referer': f'https://www.great-home.com.tw/detail/?sn={source_id}',
                    'Sec-Fetch-Dest': 'iframe',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'same-origin',
                    'Upgrade-Insecure-Requests': '1',
                    'User-Agent': self.user_agent.random
                }
                param = {'sn' : source_id}
                map_s = requests.session()
                try:
                    map_res = map_s.get(map_url, params=param, headers=map_header, proxies=proxy)
                    map_soup = BeautifulSoup(map_res.text, 'lxml')
                    map = map_soup.select_one('#main').find('script')
                    lat = round(float(re.findall('lat=(\d+\.\d+)', map.text)[0]),7)
                    lng = round(float(re.findall('lon=(\d+\.\d+)', map.text)[0]),7)
                except:
                    lat, lng = 0,0
                
                # 房仲資料格式分兩種 1.住商不動產(無房仲名字) 2.大家房屋
                contact_man, brand, branch, company, phone = '', '', '', '', ''
                contact_type = '仲介'
                is_man = soup.select_one('.contact__name')
                if is_man:
                    contact_man = is_man.text
                is_branch = soup.select_one('#MainContent_ObjectContent > div.main--R > div.item-contact > div.service__user > div.service__contact__info > p.contact__tit > a:nth-child(2)')
                if is_branch:
                    branch = is_branch.text
                is_company = soup.find('p',{'class':'contat__add'})
                if is_company:
                    company = is_company.text
                is_phone = soup.find('p',{'class':'contact__phone'})
                if is_phone:
                    phone = is_phone.text
                if not branch:
                    branch = soup.find('p',{'class':'contact__tit'}).text
                    brand = '住商不動產'
                    branch = branch.lstrip('住商不動產')
                else:
                    brand = '大家房屋'
                if total_ping!= 0:
                    price_ave = round(total/total_ping,2)
                else:
                    if land_ping!=0:
                        price_ave = round(total/land_ping,2)
                    else:
                        price_ave = 0
                pattern, pattern1 = '', ''
                is_pattern = soup.find('li',{'class':'icon_room'})
                if is_pattern:
                    pattern = is_pattern.text.strip()
                    if pattern == '建物未保存登記':
                        pattern = ''
                    elif pattern == 'OPEN':
                        pattern = '開放式'
                    is_pattern1 = is_pattern.find('span',{'class':'noteinfo'})
                    if is_pattern1:
                        pattern1 = is_pattern1.text
            
            except requests.exceptions.ProxyError as err:
                self.logger.error(f"Proxy Error processing link {link}: {str(err)}")
                time.sleep(random.uniform(5,10))
                proxy = get_proxies()
                continue

            except Exception as e:
                self.logger.error(f"Error processing link {link}: {str(e)}")
                continue
            
            # 此網站無提供以下資訊
            house_num, edge, dark = '', '', ''
            
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
            data  = clean_data(data)
            data_list.append(data)
            
            # 如果超過100筆物件，就先寫入資料庫，並清空data_list
            if len(data_list) % 100 == 0:
                data2sql(data_list, city_code)
                data_list = []

        data2sql(data_list, city_code)
        # 把物件都寫進資料庫之後，再寫入group_key跟address_cal
        find_group(self, city_code)
        find_possible_addrs(self, city_code)