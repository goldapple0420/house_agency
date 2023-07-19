import requests
from bs4 import BeautifulSoup
import urllib.request
import re
import time
import pandas as pd
from fake_useragent import UserAgent
import random
from crawler.craw_common import  *

class HouseInfo():
    def __init__(self):
        self.name = 'houseinfo'
        self.source = '房屋資訊網'
        self.user_agent = UserAgent()
        self.logger = WriteLogger().getLoggers()
        self.all_city_dict = {
        "A": {"city_c": "臺北市", "city_n": "12"},
        "B": {"city_c": "臺中市", "city_n": "89"},
        "C": {"city_c": "基隆市", "city_n": "03"},
        "D": {"city_c": "臺南市", "city_n": "1415"},
        "E": {"city_c": "高雄市", "city_n": "1617"},
        "F": {"city_c": "新北市", "city_n": "02"},
        "G": {"city_c": "宜蘭縣", "city_n": "21"},
        "H": {"city_c": "桃園市", "city_n": "04"},
        "I": {"city_c": "嘉義", "city_n": "1213"},
        "J": {"city_c": "新竹", "city_n": "56"},
        "K": {"city_c": "苗栗縣", "city_n": "07"},
        "M": {"city_c": "南投縣", "city_n": "22"},
        "N": {"city_c": "彰化縣", "city_n": "10"},
        "O": {"city_c": "新竹", "city_n": "56"},
        "P": {"city_c": "雲林縣", "city_n": "11"},
        "Q": {"city_c": "嘉義", "city_n": "1213"},
        "T": {"city_c": "屏東縣", "city_n": "18"},
        "U": {"city_c": "花蓮縣", "city_n": "20"},
        "V": {"city_c": "臺東縣", "city_n": "19"},
        "X": {"city_c": "澎湖縣", "city_n": "23"},
        "W": {"city_c": "金門縣", "city_n": "24"},
        }

    def get_house_items(self, city_code):
        list_obj = []
        if city_code == 'Z':
            # print('該網站沒有提供連江縣物件')
            return list_obj
        city_n = self.all_city_dict[city_code]['city_n']
        city = self.all_city_dict[city_code]['city_c']
        proxy = get_proxies()
        page = 0
        while True:
            page+=1
            url = f'https://www.house-info.com.tw/H{city_n}?p={page-1}' # 第一頁的p=0,第二頁p=1,以此類推
            header = {'User-Agent' : self.user_agent.random}
            try:
                time.sleep(random.uniform(1,5))
                res = requests.get(url, headers=header, proxies=proxy)
                soup = BeautifulSoup(res.text, 'lxml')
                items = soup.select_one('#divList').find_all('li',{'class':'liCase'})
                if not items:
                    # print(f'City{city_code} 總共{len(list_obj)}個物件')
                    break
                for item in items:
                    re_price = item.find('span',{'class':'cssListPrice'}).text
                    if '租' not in re_price: # 出租的不列入
                        link = 'https://www.house-info.com.tw' + item.find('a')['href'].lstrip('..')
                        source_id = item.find('a')['id']
                        subject = item.find('a').text
                        total = int(int(item.find('meta',{'itemprop':'price'})['content']) / 10000)
                        # 新竹跟嘉義要找出是在縣還是市
                        # 新竹市只有箱山區、北區、東區
                        if city == '新竹':
                            area = item.find('meta',{'itemprop':'description'})['content']
                            if ('新竹香山' in area) or ('新竹北區' in area) or ('新竹東區' in area):
                                city = '新竹市'
                            else:
                                city = '新竹縣'
                        # 嘉義市只有東區、西區
                        elif city == '嘉義':
                            area = item.find('meta',{'itemprop':'description'})['content']
                            if ('嘉義西區' in area) or ('嘉義東區' in area):
                                city = '嘉義市'
                            else:
                                city = '嘉義縣'
                        obj_data = {
                                    "source_id" : source_id,
                                    "subject" : subject,
                                    "link" : link,
                                    "city" : city,
                                    "re_price" : total,
                                    "insert_time" : time.strftime("%Y-%m-%d %H:%M:%S")
                                    }
                        list_obj.append(obj_data)
            
            except requests.exceptions.ProxyError as e:
                self.logger.error(f"ProxyError processing House City{city_code} Page{page}: {str(e)}")
                time.sleep(random.uniform(3,10))
                proxy = get_proxies()
            
            except Exception as e:
                self.logger.error(f"Error processing House City{city_code} Page{page}: {str(e)}")   
        
        if list_obj:
            to_sqlite(self, list_obj)
        else: # 是空的也要建立空資料表
            con = sqlite3.connect(os.path.abspath('compare.db'))
            table_name = f'{self.name}_{time.strftime("%m%d")}'
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} (source_id, subject, link, city, re_price, insert_time)"
            con.execute(sql)
            con.close()

    def get_land_items(self, city_code):
        list_obj = []
        if city_code == 'Z':
            # print('該網站沒有提供連江縣物件')
            return list_obj
        city_n = self.all_city_dict[city_code]['city_n']
        city = self.all_city_dict[city_code]['city_c']
        proxy = get_proxies()
        page = 0
        while True:
            page+=1
            url = f'https://www.house-info.com.tw/L{city_n}?p={page-1}' # 第一頁的p=0,第二頁p=1,以此類推
            header = {'User-Agent' : self.user_agent.random}
            try:
                time.sleep(random.uniform(1,5))
                res = requests.get(url, headers=header, proxies=proxy)
                soup = BeautifulSoup(res.text, 'lxml')
                items = soup.select_one('#divList').find_all('li',{'class':'liCase'})
                if not items:
                    # print(f'City{city_code} 總共{len(list_obj)}個物件')
                    break
                for item in items:
                    re_price = item.find('span',{'class':'cssListPrice'}).text
                    if '租' not in re_price: # 出租的不列入
                        link = 'https://www.house-info.com.tw' + item.find('a')['href'].lstrip('..')
                        source_id = item.find('a')['id']
                        subject = item.find('a').text
                        total = int(item.find('meta',{'itemprop':'price'})['content']) / 10000
                        obj_data = {
                                    "source_id" : source_id,
                                    "subject" : subject,
                                    "link" : link,
                                    "city" : city,
                                    "re_price" : total,
                                    "insert_time" : time.strftime("%Y-%m-%d %H:%M:%S")
                                    }
                        list_obj.append(obj_data)
            
            except requests.exceptions.ProxyError as e:
                self.logger.error(f"ProxyError processing House City{city_code} Page{page}: {str(e)}")
                time.sleep(random.uniform(3,10))
                proxy = get_proxies()

            except Exception as e:
                self.logger.error(f"Error processing Land City{city_code} Page{page}: {str(e)}")   

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
        headers = {'User-Agent' : self.user_agent.random}
        proxy = get_proxies()
        for obj in list_obj:
            source_id = obj['source_id']
            subject = obj['subject']
            link = obj['link']
            total = obj['re_price']
            city = obj['city']
            print(subject, link)
            time.sleep(random.uniform(1,3))
            try:
                res = requests.get(link, headers=headers, proxies=proxy)
                soup = BeautifulSoup(res.text, 'lxml')
                address = soup.select_one('#spanCaseFullAddr').text.replace('台','臺')
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
                house_type = soup.find('meta',{'itemprop':'category'})['content']
                img_url = soup.find('meta',{'itemprop':'image'})['content']
                if 'www' not in img_url:
                    img_url = ''
                contact_type, contact_man, phone, brand, branch, company = '', '', '', '', '', ''
                house_age, house_age_v  ='', 0
                pattern, community, parking_type, manage_type, manage_fee, blockto, edge, feature = '', '', '', '', '', '', '', ''
                contact_type, contact_man, phone
                floor_web, floor, total_floor = '', 0, 0
                total_ping, building_ping, att_ping, public_ping, land_ping = 0,0,0,0,0
                all_detail = soup.select_one('#divCaseDetail')
                match = re.findall('屋齡 (\d+) 年', all_detail.text)
                if match:
                    house_age = match[0] + ' 年'
                    house_age_v = int(match[0])
                details = all_detail.find_all('tr')
                for detail in details:
                    if len(detail.find_all('th')) >1:
                        if detail.find('th').text == '格局':
                            pattern = detail.find('td').text.replace('，','')
                        if detail.find_all('th')[1].text == '社區/大樓':
                            community = detail.find_all('td')[1].text
                        if detail.find('th').text == '車位':
                            parking_type = detail.find('td').text
                        if detail.find('th').text == '管委會':
                            manage_type = detail.find('td').text
                        if detail.find_all('th')[1].text == '管理費':
                            manage_fee = detail.find_all('td')[1].text
                        if detail.find_all('th')[1].text == '座向':
                            blockto = detail.find_all('td')[1].text
                        if detail.find('th').text == '邊間':
                            edge = detail.find('td').text
                        if detail.find('th').text == '房屋銷售來源':
                            contact_type = detail.find('td').text
                        if detail.find('th').text == '手機':
                            phone = detail.find('td').text
                        if detail.find('th').text == '所屬公司名稱':
                            agent = detail.find('td').text
                            is_brand = re.findall(r'[\u4e00-\u9fff]+[房屋|不動產]', agent)
                            if is_brand:
                                brand = is_brand[0]
                            is_branch = re.findall(rf'([\u4e00-\u9fff]+店)', agent)
                            if is_branch:
                                branch = is_branch[0]
                                if brand:
                                    if brand in branch:
                                        branch = branch.replace(brand, '')
                            is_company = re.findall(r'[\u4e00-\u9fff]+公司', agent)
                            if is_company:
                                company = is_company[0]
                        if detail.find('th').text == '建物所在樓層':
                            floors = detail.find('td').text
                            if '/' in floors:
                                floor_web = floors.split('/')[0]
                                ttmatch = re.findall('\d+', floors.split('/')[1])
                                if ttmatch:
                                    total_floor = int(ttmatch[0])
                            else:
                                floor_web = floors
                                total_floor = 0
                            if 'B' in floor_web:
                                floor = -1
                            else:
                                match = re.findall('\d+', floor_web)
                                if match:
                                    floor = int(match[0])
                        if detail.find('th').text == '每坪單價':
                            if '萬' in detail.find('td').text:
                                match = re.findall('(\d+\.?\d*)萬/坪',detail.find('td').text)
                                if match:
                                    price_ave = float(match[0])
                            elif '元' in detail.find('td').text:
                                match = re.findall('(\d+\.?\d*)元/坪',detail.find('td').text)
                                if match:
                                    price_ave = float(match[0]) / 10000
                        if detail.find_all('th')[1].text == '總登記坪數':
                            match = re.findall('(\d+\.?\d*)',detail.find_all('td')[1].text)
                            if match:
                                total_ping = float(match[0])
                        if detail.find('th').text == '主建物坪數':
                            match = re.findall('(\d+\.?\d*)',detail.find('td').text)
                            if match:
                                building_ping = float(match[0])
                        if detail.find('th').text == '附屬建物坪數':
                            match = re.findall('(\d+\.?\d*)',detail.find('td').text)
                            if match:
                                att_ping = float(match[0])
                        if detail.find_all('th')[1].text == '土地登記總坪數':
                            match = re.findall('(\d+\.?\d*)',detail.find_all('td')[1].text)
                            if match:
                                land_ping = float(match[0])
                        if detail.find_all('th')[1].text == '公共設施坪數':
                            match = re.findall('(\d+\.?\d*)',detail.find_all('td')[1].text)
                            if match:
                                public_ping = float(match[0])
                    elif detail.find('th'):
                        if detail.find('th').text == '房屋特色':
                            feature = detail.find('td').text
                        elif detail.find('th').text == '聯絡人姓名':
                            contact_man = detail.find('td').text.split(r'（')[0]
                lat, lng = 0,0
                map = soup.select_one('#iframeGoogleMap')
                if map:
                    match = re.search('q=(\d+.\d+),(\d+.\d+)',map['src'])
                    lat = match.group(1)
                    lng = match.group(2)
                if not price_ave:
                    if total_ping != 0:
                        price_ave = round((total/total_ping),2)
                    else:
                        if land_ping != 0:
                            price_ave = round((total/land_ping),2)
                        else:
                            price_ave = 0
            
            except requests.exceptions.ProxyError as err:
                self.logger.error(f"ProxyError processing link {link}: {str(err)}")
                time.sleep(random.uniform(5,10))
                proxy = get_proxies()
                continue
            
            except Exception as err:
                self.logger.error(f"Error processing link {link}: {str(err)}")
                continue
            
            # 此網站無提供以下資訊
            house_num, situation, dark, pattern1 = '', '', '', ''

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