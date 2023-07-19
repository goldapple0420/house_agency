import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import random
from fake_useragent import UserAgent
from crawler.craw_common import *


class Century21:
    def __init__(self):
        self.name = 'century21'
        self.source = '21世紀'
        self.user_agent = UserAgent()
        self.logger = WriteLogger().getLoggers()
        self.all_city_dict = {
        "A": {"city_c": "臺北市", "city_e": "taipei-city"},
        "B": {"city_c": "臺中市", "city_e": "taichung-city"},
        "C": {"city_c": "基隆市", "city_e": "keelung-city"},
        "D": {"city_c": "臺南市", "city_e": "tainan-city"},
        "E": {"city_c": "高雄市", "city_e": "kaohsiung-city"},
        "F": {"city_c": "新北市", "city_e": "new-taipei-city"},
        "G": {"city_c": "宜蘭縣", "city_e": "yilan-county"},
        "H": {"city_c": "桃園市", "city_e": "taoyuan-city"},
        "I": {"city_c": "嘉義市", "city_e": "chiayi-city"},
        "J": {"city_c": "新竹縣", "city_e": "hsinchu-county"},
        "K": {"city_c": "苗栗縣", "city_e": "miaoli-county"},
        "M": {"city_c": "南投縣", "city_e": "nantou-county"},
        "N": {"city_c": "彰化縣", "city_e": "changhua-county"},
        "O": {"city_c": "新竹市", "city_e": "hsinchu-city"},
        "P": {"city_c": "雲林縣", "city_e": "yunlin-county"},
        "Q": {"city_c": "嘉義縣", "city_e": "chiayi-county"},
        "T": {"city_c": "屏東縣", "city_e": "pingtung-county"},
        "U": {"city_c": "花蓮縣", "city_e": "hualien-county"},
        "V": {"city_c": "臺東縣", "city_e": "taitung-county"},
        "X": {"city_c": "澎湖縣", "city_e": "penghu-county"},
        "W": {"city_c": "金門縣", "city_e": "kinmen-county"},
        "Z": {"city_c": "連江縣", "city_e": "lienchiang-county"}
        }

    def get_items(self, city_code):
        city_e = self.all_city_dict[city_code]['city_e']
        city = self.all_city_dict[city_code]['city_c']
        
        header = {'User-Agent': self.user_agent.random}
        proxies = get_proxies()
        page = 0
        list_obj = []
        while True:
            page+=1
            page_url = f'https://www.century21.com.tw/buy/{city_e}/all-district/all-category?page={page}'
            try:
                res = requests.get(page_url, headers=header, proxies=proxies)
                soup = BeautifulSoup(res.text, 'lxml')
                items = soup.find_all('div',{'class':'list_info'})
                if not items:
                    break
                for item in items:
                    link = item.find('a')['href']
                    source_id = re.search(r'/buypage/(\d+)', link).group(1)
                    subject = item.find('a')['title']
                    re_price = int(float(item.find('div',{'class':'new-price'}).text.rstrip('萬').replace(',','')))
                    obj_data = {
                            "source_id" : source_id,
                            "subject" : subject,
                            "link" : link,
                            "city" : city,
                            "re_price" : int(re_price),
                            "insert_time" : time.strftime("%Y-%m-%d %H:%M:%S")
                            }
                    list_obj.append(obj_data)
            
            except requests.exceptions.HTTPError as e:
                self.logger.error(f"HTTP error occurred: {str(e)}")
                time.sleep(random.uniform(1,5))
                continue
            
            except requests.exceptions.ProxyError as e:
                self.logger.error(f"Error Proxy Source {self.source}  Proxy:{proxies} : {str(e)}")
                proxies = get_proxies()
                header = {'User-Agent': self.user_agent.random}
            
            except Exception as e:
                self.logger.error(f"Error processing City {city_code} Page {page}: {str(e)}")
                continue
        
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
        # 取得新資料進來爬更多詳細資料
        list_obj = is_new(self, city_code)
        proxy = get_proxies()
        data_list = []
        for obj in list_obj:
            header = {'User-Agent': self.user_agent.random }
            time.sleep(random.randint(3,10))
            link = obj['link']
            subject = obj['subject']
            source_id = obj['source_id']
            total = obj['re_price']
            city = obj['city']
            print(subject, link)
            try:
                res = requests.get(link, headers=header, proxies=proxy)
                if '無此網頁' in res.text:
                    print('很抱歉, 您輸入的網頁已經不存在。')
                    continue
                soup = BeautifulSoup(res.text, 'lxml')
                address = soup.find('h3',{'class':'address'}).text.replace('台','臺')
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
                is_feature = soup.find('div',{'class':'full'})
                feature = is_feature.text if is_feature else ''
                price_ave, house_type, situation, total_ping = '', '', '', 0
                floor_web, floor, total_floor = '', 0, 0
                pattern, parking_type, house_num, blockto = '', '', '', ''
                house_age, house_age_v = '', 0
                manage_type, manage_fee = '', ''
                building_ping, att_ping, public_ping, land_ping = 0,0,0,0
                infos = soup.find_all('div',{'class':'w-half'})
                for i in range(len(infos)):
                    if infos[i].find('span').text == '參考單價':
                        match = re.findall('\d+\.*\d*', infos[i].find('div').text)
                        if match:
                            price_ave = float(match[0])
                    elif infos[i].find('span').text == '類型/用途':
                        house_type = infos[i].find('div').text.split('[')[0]
                    elif infos[i].find('span').text == '法定用途':
                        situation = infos[i].find('div').text
                    elif infos[i].find('span').text == '坪數':
                        total_ping = float(infos[i].find('div').text.replace(',','').rstrip(' 坪'))
                    elif infos[i].find('span').text == '格局':
                        pattern = infos[i].find('div').text
                    elif infos[i].find('span').text == '車位':
                        parking_type = infos[i].find('div').text
                    elif infos[i].find('span').text == '屋齡':
                        house_age = infos[i].find('div').text
                        house_age_v = float(re.findall('(\d+\.*\d*) 年', house_age)[0])
                    elif infos[i].find('span').text == '單層戶數':
                        house_num = infos[i].find('div').text.rstrip('戶')
                    elif infos[i].find('span').text == '朝向':
                        blockto = infos[i].find('div').text
                    elif infos[i].find('span').text == '警衛管理':
                        manage_type = infos[i].find('div').text
                    elif infos[i].find('span').text == '警衛費':
                        manage_fee = infos[i].find('div').text
                    elif infos[i].find('span').text == '主建物面積':
                        building_ping = float(infos[i].find('div').text.replace(',','').rstrip(' 坪'))
                    elif infos[i].find('span').text == '附屬建物面積':
                        att_ping = float(infos[i].find('div').text.rstrip(' 坪'))
                    elif infos[i].find('span').text == '土地持有面積':
                        land_ping = float(infos[i].find('div').text.replace(',','').rstrip(' 坪'))
                    elif infos[i].find('span').text == '共同使用部分面積':
                        public_ping = float(infos[i].find('div').text.replace(',','').rstrip(' 坪'))
                    elif infos[i].find('span').text == '樓層':
                        floors = infos[i].find('div').text
                        floor_web = floors.split('/')[0].strip()
                        total_floors = floors.split('/')[0].strip()
                        if total_floors:
                            match = re.findall('\d+',total_floors)
                            if match:
                                total_floor = int(match[0])
                        if floor_web:
                            if 'B' in floor_web:
                                floor = -1
                            else:
                                match = re.findall('\d+',floor_web)
                                if match:
                                    floor = int(match[0])
                # 如果網頁無提供單價，自己計算
                if not price_ave:
                    if total_ping!= 0:
                        price_ave = round(total/total_ping,2)
                    else:
                        if land_ping!=0:
                            price_ave = round(total/land_ping,2)
                        else:
                            price_ave = 0
                contact_man, branch, phone = '','',''
                agent = soup.find('div',{'class':'agent'})
                if agent:
                    contact_man = agent.find('h5',{'class':'name'}).text
                    branch = agent.find('h5',{'class':'address'}).text
                    is_phone = agent.find('h5',{'class':'tel'})
                    if is_phone:
                        phone = is_phone.text
                contact_man = contact_man.rstrip(';')
                branch = branch.rstrip(';')
                phone = phone.rstrip('; ')
                brand = '21世紀'
                company = soup.find('ul',{'class':'unstyled'}).find('li').text
                img = soup.find('meta',{'property':'og:image'})
                img_url = 'https:' + img['content']
                if img['content'] == '//www.century21.com.tw/uploads/J297/7576367/Ai.jpg':
                    img_url = ''
                scripts = soup.find_all('script')
                map = scripts[5]
                match = re.search('var obj_y = (\d+\.\d+);\s+var obj_x = (\d+\.\d+);', map.text)
                if match:
                    lat = round(float(match.group(1)),7)
                    lng = round(float(match.group(2)),7)
                else:
                    lat, lng = 0,0
            
            except requests.exceptions.ProxyError as err:
                self.logger.error(f"Proxy Error processing link {link}: {str(err)}")
                time.sleep(random.uniform(5,10))
                proxy = get_proxies()
                continue
            
            except Exception as err:
                self.logger.error(f"Error processing link {link}: {str(err)}")
                continue

            # 該網站無提供以下資訊
            pattern1, contact_type, edge, dark, community = '', '', '', '', ''
            
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
