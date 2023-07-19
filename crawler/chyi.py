import requests
from bs4 import BeautifulSoup
import pandas as pd
import re,time, random
from fake_useragent import UserAgent
from crawler.craw_common import *

class Chyi():
    def __init__(self):
        self.name = 'chyi'
        self.source = '群義房屋'
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
        list_obj = []
        city_n = self.all_city_dict[city_code]['city_n']
        city = self.all_city_dict[city_code]['city_c']
        
        url = 'https://www.chyi.com.tw/Function/Ajax/SearchObj.aspx'
        header = {'User-Agent': self.user_agent.random}
        proxies = get_proxies()
        page, last_page = 0, 1
        while page<last_page:
            page+=1
            data = {
                'HotObj': '0',
                'TabID': 'Obj_List',
                'RS_Type': '2',
                'CityID': city_n,
                'AreaID_List': '',
                'EventType': 'AllObj',
                'Pg1_ID': 'Pg1',
                'Pg1': str(page),
                'OrderS_ID': 'OrderS',
                'OrderS': '委託起日_A',
                'RawUrl': '/sell_item',
                'rnd': '0.9073819434370043'
                }
            s = requests.session()
            try:
                time.sleep(random.uniform(1,2))
                res = s.post(url, data=data, headers=header, proxies=proxies)
                soup = BeautifulSoup(res.text, 'lxml')
                items = soup.find_all('li',{'class':'house_block'})
                # print(f'Source{self.source} City{city} Page{page}共{len(items)}個物件')
                for i in items:
                    item = i.find('a',{'target':'_blank'})
                    link = 'https://www.chyi.com.tw' + item['href']
                    subject = item.text
                    source_id = re.findall('/sell_item/(.+)', link)[0].strip('/')
                    price1 = i.find('dd',{'class':'ShowBuy'}).find('h4').get_text(strip=True)
                    match = re.findall('總價(\d+)\.?\d*萬', price1)
                    if match:
                        re_price = int(match[0])
                    else:
                        re_price = int(float(i.find('span',{'class':'discount'}).text))
                    obj_data = {
                                "source_id" : source_id,
                                "subject" : subject,
                                "link" : link,
                                "city" : city,
                                "re_price" : int(re_price),
                                "insert_time" : time.strftime("%Y-%m-%d %H:%M:%S")
                                }
                    list_obj.append(obj_data)
                
                pages = soup.find('ul',{'class':'page'})
                if pages:
                    last_page = int(re.findall(f'顯示頁數 \d+? - (\d+?)\s',pages.text)[0])
            
            except requests.exceptions.ProxyError as e:
                self.logger.error(f"Proxy Error Source {self.source}  Page:{page} : {str(e)}")
                time.sleep(random.uniform(10,15))
                proxies = get_proxies()
                header = {'User-Agent': self.user_agent.random}

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
        return list_obj

    def get_data(self, city_code):
        list_obj = is_new(self, city_code)
        data_list = []
        proxies = get_proxies()
        for obj in list_obj:
            subject = obj['subject']
            link = obj['link']
            source_id = obj['source_id']
            total = obj['re_price']
            print(subject, link)
            user_agent=UserAgent()
            header = {'User-Agent': user_agent.random }
            time.sleep(random.uniform(1,2))
            try:
                res = requests.get(link, headers=header, proxies=proxies)
                if '下架' in res.text:
                    # print('連結出錯了，或已下架！')
                    continue
                soup = BeautifulSoup(res.text, 'lxml')
                objs = soup.find('div',{'class':'ObjectMsg'})
                agent = objs.text.split('\n')[13]
                brand = soup.select_one('#og_brand')['content']
                branch = agent.split(r'(')[0]
                match = re.search(r'店\((.+公司)', agent)
                company = match.group(1) if match else ''
                phone = objs.text.split('\n')[14]
                city = obj['city']
                area = soup.select_one('#MainTitle > dt.breadList > a:nth-child(3)').text
                main_info = soup.find('div',{'class':'item_main_info'})
                try:
                    road = re.findall(f'{area}(.+?)\n', main_info.text)[0]
                except:
                    road =''
                address = city + area + road
                type = re.findall('型態/類別(.+?)\n', main_info.text)[0]
                house_type = type.split('/')[0].strip()
                situation = type.split('/')[1]
                floor_web, floor, total_floor = '', 0, 0
                match = re.findall('樓別/樓高(.+?)\n', main_info.text)
                if match:
                    floors = match[0]
                    floor_web = floors.split('/')[0].strip()
                    total_floors = floors.split('/')[1].strip()
                    if total_floors:
                        total_floor = int(re.findall('\d+',total_floors)[0])
                    if floor_web:
                        if 'B' in floor_web:
                            floor = -1
                        elif '-' in floor_web:
                            match = re.findall('\d+',floor_web.split('-')[0])
                            if match:
                                floor = int(match[0])
                        else:
                            match = re.findall('\d+',floor_web)
                            if match:
                                floor = int(match[0])
                pattern, pattern1 = '', ''
                match = re.findall('格局(.+?)格局', main_info.text)
                if match:
                    pattern = match[0]
                    if '(另加蓋' in pattern:
                        pattern, pattern1 = pattern.split(r'(')
                        pattern1 = pattern1.rstrip(r')')
                match = re.findall('屋齡(.+?) 年屋齡', main_info.text)
                if match:
                    house_age = match[0]
                    house_age_v = float(house_age)
                else:
                    house_age, house_age_v = '', 0
                match = re.findall(r'權狀坪(.+?) 坪', main_info.text)
                if match:
                    total_ping = float(match[0].lstrip(' (含車位)'))
                else:
                    total_ping = 0
                is_feature = soup.find('ul',{'class':'item_features Characteristic_text'})
                if is_feature:
                    feature = is_feature.text.strip()
                    if not feature:
                        feature = ''
                parking_type, manage_fee, blockto = '', '', ''
                details = soup.find_all('div',{'class':'detailed_text'})
                match = re.findall('車位/備註(.+?)\s', details[0].text)
                if match:
                    parking_type = match[0]
                match = re.findall('管理費(.+?)\s', details[0].text)
                if match:
                    manage_fee = match[0]
                match = re.findall('入門朝向(.+?)\s', details[0].text)
                if match:
                    blockto = match[0]
                building_ping, att_ping, public_ping, land_ping = 0,0,0,0
                match = re.findall('主建物(.+?)坪', details[1].text)
                if match:
                    building_ping = float(match[0])
                match = re.findall('附屬建物(.+?)坪', details[1].text)
                if match:
                    att_ping = float(match[0])
                match = re.findall('公共設施(.+?)坪', details[1].text)
                if match:
                    public_ping = float(match[0])
                match = re.findall('地坪(.+?)坪', details[1].text)
                if match:
                    land_ping = float(match[0])
                if total_ping!= 0:
                    price_ave = round(total/total_ping,2)
                else:
                    if land_ping!=0:
                        price_ave = round(total/land_ping,2)
                    else:
                        price_ave = 0
                map = soup.select_one('#ObjectInfo > dd:nth-child(8) > div > script')
                if map:
                    match = re.findall('defLat=(\d+.\d+)', map.text)
                    match1 = re.findall(';defLng=(\d+.\d+)', map.text)
                    if match:
                        lat = round(float(match[0]),7)
                        lng = round(float(match1[0]),7)
                else:
                    map = soup.select_one('#ObjectInfo > dd:nth-child(6) > div > script')
                    if map:
                        match = re.findall('defLat=(\d+.\d+)', map.text)
                        match1 = re.findall(';defLng=(\d+.\d+)', map.text)
                        if match:
                            lat = round(float(match[0]),7)
                            lng = round(float(match1[0]),7)
                    else:
                        lat, lng = 0,0
                img_url = soup.select_one('#og_image')['content']
                if 'logo/fb_' in img_url: # 無圖片的物件會放上此jpg
                    img_url = ''
                elif 'nopic' in img_url: # 無圖片的物件會放上此gif
                    img_url = ''
            
            except Exception as err:
                self.logger.error(f"Error processing link {link}: {str(err)}")
                continue
            
            # 此網站無提供以下資訊
            house_num, manage_type, edge, dark, contact_type, contact_man, community = '', '', '', '', '', '', ''
            
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