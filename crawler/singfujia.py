import requests
from bs4 import BeautifulSoup
import pandas as pd
import os,re,time,json
from fake_useragent import UserAgent
import random
from crawler.craw_common import  *

class SingFuJija():
    def __init__(self):
        self.name = 'singfujia'
        self.source = '幸福家不動產'
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
        city_n = city.replace('臺','台')
        proxy = get_proxies()
        list_obj = []
        page = 0
        while True:
            page+=1
            page_url = f'https://singfujia.com/?nav=buy&q=menu%3Dbuy%26pageVal%3D{page}%26class_number%3Dbuy%26cityString%3D{city_n}%26city%5B%5D%3D{city_n}%26undefined%3D'
            headers = {'user-agent':self.user_agent.random}
            try:
                time.sleep(random.uniform(1,3))
                res = requests.get(page_url, headers=headers, proxies=proxy)
                print(f'開始擷取{self.source} {city} P{page} 物件')
                soup = BeautifulSoup(res.text, 'lxml')
                items = soup.find_all('div',{'class':'info'})
                if not items:
                    # print(f'City {city_code} 共{len(list_obj)}個物件')
                    break
                for item in items:
                    source_id = item.find('p').text.lstrip('S')
                    link = f'https://singfujia.com/?nav=buy_d&id={source_id}'
                    subject = item.find('h3').text
                    re_price = int(float(item.find('p',{'class':'price'}).find('i').text.replace(',','')))
                    obj_data = {
                            "source_id" : source_id,
                            "source" : self.source,
                            "subject" : subject,
                            "link" : link,
                            "city" : city,
                            "re_price" : re_price,
                            "insert_time" : time.strftime("%Y-%m-%d %H:%M:%S")
                            }
                    list_obj.append(obj_data)
            
            except requests.exceptions.ProxyError as err:
                self.logger.error(f"Proxy Error processing {city} Page {page}: {str(err)}")
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
        proxy = get_proxies()
        for obj in list_obj:
            try:
                subject = obj['subject']
                link = obj['link']
                source_id = obj['source_id']
                print(subject, link)
                header = {'User-Agent': self.user_agent.random }
                time.sleep(random.uniform(2,5))
                res = requests.get(link, headers=header, proxies=proxy)
                if '查無此物件' in res.text:
                    continue
                soup = BeautifulSoup(res.text, 'lxml')
                title = soup.find('div',{'class':'titleInfo'})
                subject, address = title.text.strip().split('\n')
                address = address.replace('台','臺')
                area, road = '', ''
                city = obj['city']
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
                #total = int(soup.find('p',{'class':'price'}).text.rstrip('萬').replace(',',''))
                total = soup.find('p',{'class':'price'})
                total = int(float(total.find('i').text.replace(',','')))
                is_feature = soup.find('div',{'id':'tab2'})
                feature = is_feature.text.strip() if is_feature else ''
                is_detail = soup.find('div',{'class':'detail'})
                house_type, pattern, blockto, manage_type, manage_fee, parking_type = '', '', '', '', '', ''
                total_ping, sum_ping, att_ping, public_ping, land_ping, building_ping = 0,0,0,0,0,0
                floor_web, floor, total_floor = '', 0, 0
                house_age, house_age_v, price_ave = '', 0, 0
                if is_detail:
                    match = re.findall('建物類別\n(.+?)\n', is_detail.text)
                    if match:
                        house_type = match[0]
                    match = re.findall('建物格局\n(.+?)\n', is_detail.text)
                    if match:
                        pattern = match[0]
                    match = re.findall('建物方位\n(.+?)\n', is_detail.text)
                    if match:
                        blockto = match[0].replace('-','').replace(' / ','')
                    match = re.findall('管理方式\n(.+?)\n', is_detail.text)
                    if match:
                        manage_type = match[0]
                    match = re.findall('管理費\n(.+?)\n', is_detail.text)
                    if match:
                        manage_fee = match[0]
                    match = re.findall('車位\n(.+?)\n', is_detail.text)
                    if match:
                        parking_type = match[0]
                    # 坪數
                    match = re.findall('登記建坪\n(\d+\.?\d*)坪', is_detail.text)
                    if match:
                        total_ping = float(match[0])
                    match = re.findall('主建物＋附屬建物\n(\d+\.?\d*)坪', is_detail.text)
                    if match:
                        sum_ping = float(match[0])
                    match = re.findall('附屬建物\n(\d+\.?\d*)坪', is_detail.text)
                    if match:
                        att_ping = float(match[0])
                    match = re.findall('公設\n(\d+\.?\d*)坪', is_detail.text)
                    if match:
                        public_ping = float(match[0])
                    match = re.findall('登記地坪\n\n(\d+\.?\d*)\n?坪', is_detail.text)
                    if match:
                        land_ping = float(match[0])
                    building_ping = sum_ping - att_ping # 網頁上提供的是主建物+附屬建物，可自己算出
                    # 樓層
                    match = re.findall('所在樓層\n(\w+樓)', is_detail.text)
                    if match:
                        floor_web = match[0]
                        if 'B' in floor_web:
                            floor = -1
                        else:
                            match = re.findall('\d+', floor_web)
                            if match:
                                floor = int(match[0])
                    match = re.findall('地上樓層\n(\w+)樓', is_detail.text)
                    if match:
                        match2 = re.findall('\d+', match[0])
                        if match2:
                            total_floor = int(match2[-1])
                    # 屋齡
                    match = re.findall('建物屋齡\n(.+?)\n', is_detail.text)
                    if match:
                        house_age = match[0]
                        match2 = re.findall('\d+\.?\d*', house_age)
                        if match2:
                            house_age_v = float(match2[0])
                    # 單價
                    match = re.findall('每坪單價\n約(\d+\.?\d*)萬 / 坪', is_detail.text)
                    if match:
                        price_ave = float(match[0])
                    else: # 若沒提供，自己算
                        if total_ping!= 0:
                            price_ave = round(total/total_ping,2)
                        else:
                            if land_ping!=0:
                                price_ave = round(total/land_ping,2)
                img = soup.find('img',{'class':'noCropLazy'})
                img_url = img['data-src'] if img else ''
                is_phone = soup.select_one('#onlineService > div.titleBar > div > p > a')
                phone = is_phone.text if is_phone else ''
                contact = soup.select_one('#content > div.rwdFormBtns > div > div > p')
                if contact:
                    shop = contact.text.strip().split('\n')[0]
                    brand, branch = shop.split('-')
                    company = contact.text.strip().split('\n')[1].strip()
                # 經緯度
                map_url = 'https://singfujia.com/map.php'
                s = requests.session()
                map_data = {'nav': 'buy_d',
                'mt': 'realty_d',
                'id': source_id}
                map_header = {'user-agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'}
                resp = s.post(map_url, data=map_data, headers=map_header)
                if resp.status_code==200:
                    map_soup = BeautifulSoup(resp.text, 'lxml')
                    map = map_soup.select_one('#mapCanvasInside')
                    try:
                        lat = map.get_attribute_list('data-lat')[0]
                        lng = map.get_attribute_list('data-lng')[0]
                    except:
                        lat, lng = 0,0
            
            except requests.exceptions.ProxyError as err:
                self.logger.error(f"Proxy Error processing link {link}: {str(err)}")
                time.sleep(random.uniform(15,30))
                proxy = get_proxies()
                continue

            except Exception as e:
                self.logger.error(f"Error processing link {link}: {str(e)}")
                continue
            
            # 該網站無提供以下資訊
            situation, pattern1, house_num, contact_type, contact_man, edge, dark, community = '', '', '', '', '', '', '', ''

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