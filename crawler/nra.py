import requests
from bs4 import BeautifulSoup
import pandas as pd
import re,time,json
from fake_useragent import UserAgent
import random
from crawler.craw_common import  *

class Nra():
    def __init__(self):
        self.name = 'nra'
        self.source = '全國不動產'
        self.user_agent = UserAgent()
        self.logger = WriteLogger().getLoggers()
        # 新竹縣跟新竹市會一起，嘉義縣跟嘉義市會一起，無提供連江縣
        self.all_city_dict = {
        "A": {"city_c": "臺北市", "city_n": "1"},
        "B": {"city_c": "臺中市", "city_n": "8"},
        "C": {"city_c": "基隆市", "city_n": "2"},
        "D": {"city_c": "臺南市", "city_n": "13"},
        "E": {"city_c": "高雄市", "city_n": "14"},
        "F": {"city_c": "新北市", "city_n": "3"},
        "G": {"city_c": "宜蘭縣", "city_n": "4"},
        "H": {"city_c": "桃園市", "city_n": "6"},
        "I": {"city_c": "嘉義市", "city_n": "11"},
        "J": {"city_c": "新竹縣", "city_n": "5"},
        "K": {"city_c": "苗栗縣", "city_n": "7"},
        "M": {"city_c": "南投縣", "city_n": "10"},
        "N": {"city_c": "彰化縣", "city_n": "9"},
        "O": {"city_c": "新竹市", "city_n": "5"},
        "P": {"city_c": "雲林縣", "city_n": "12"},
        "Q": {"city_c": "嘉義縣", "city_n": "11"},
        "T": {"city_c": "屏東縣", "city_n": "16"},
        "U": {"city_c": "花蓮縣", "city_n": "18"},
        "V": {"city_c": "臺東縣", "city_n": "17"},
        "X": {"city_c": "澎湖縣", "city_n": "15"},
        "W": {"city_c": "金門縣", "city_n": "19"}
        }

    def get_items(self, city_code):
        list_obj = []
        if city_code == 'Z':
            # print('該網站沒有提供連江縣物件')
            return list_obj
        city_n = self.all_city_dict[city_code]['city_n']
        proxy = get_proxies()
        page = 0
        while True:
            page+=1
            url = f'https://www.nra.com.tw/buying/bsearch.php?page={page}&search_ajax=1&search_type=1&city={city_n}&sort_type=detail_day%20DESC'
            header = { 'User-Agent' : 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36' }
            try:
                time.sleep(random.uniform(2,4))
                res = requests.get(url, headers=header, proxies=proxy)
                print(f'開始擷取{self.source} City_{city_code} P{page} 物件')
                soup = BeautifulSoup(res.text, 'lxml')
                items = soup.find_all('div',{'class':'obj_details_wrap'})
                if not items:
                    break
                for item in items:
                    link = 'https://www.nra.com.tw/buying/' + item.find('a')['href']
                    source_id = re.findall('num=(\d+)', link)[0]
                    subject = item.find('span',{'class':'name limited_text'}).text
                    # 抓正確的city
                    city = item.find('span',{'class':'add limited_text'}).text[:3]
                    re_price = item.find('span',{'class':'price_n'}).get_text(strip=True)
                    re_price = int(re_price.replace(',','').rstrip('萬'))
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
                self.logger.error(f"Proxy Error processing City {city_code} Page{page}: {str(err)}")
                time.sleep(random.uniform(5,10))
                proxy = get_proxies()

            except Exception as e:
                self.logger.error(f"Error processing City {city_code} Page{page}: {str(e)}")
        
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
            subject = obj['subject']
            link = obj['link']
            source_id = obj['source_id']
            city = obj['city']
            total = obj['re_price']
            header = { 'User-Agent' : self.user_agent.random}
            time.sleep(random.uniform(3, 7))
            for _ in range(3):
                try:
                    print(subject, link)
                    res = requests.get(link, headers=header, proxies=proxy)
                    if res.status_code == 200:
                        soup = BeautifulSoup(res.text, 'lxml')
                        address = soup.find('span',{'class':'add'}).text.replace('台','臺')
                        break
                except:
                    print(f'{self.source} ID:{source_id} 連線失敗，重新連線中...請等待15~30秒')
                    time.sleep(random.randint(15, 30))
                    proxy = get_proxies()
                    header = { 'User-Agent' : self.user_agent.random}
            try:
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
                total = float(soup.find('span',{'class':'price_n'}).text.replace(',','').strip().rstrip('萬'))
                total_ping, building_ping, att_ping, public_ping, land_ping = 0,0,0,0,0
                house_type, pattern, pattern1, house_num, blockto = '', '', '', '', ''
                manage_type, manage_fee, parking_type, situation, contact_man = '', '', '', '', ''
                house_age, house_age_v, floor_web, floor, total_floor = '', 0, '', 0, 0
                info = soup.find('div',{'class':'obj_big_detail1 obj_big_details col-sm-12'})
                match = re.findall('建物登記：(\d+\.*\d*)坪', info.text)
                if match:
                    total_ping = float(match[0])
                match = re.findall('土地登記：(\d+\.*\d*)坪', info.text)
                if match:
                    land_ping = float(match[0])
                match = re.findall('主建物面積：(\d+\.*\d*)坪', info.text)
                if match:
                    building_ping = float(match[0])
                match = re.findall('附屬建物面積：(\d+\.*\d*)坪', info.text)
                if match:
                    att_ping = float(match[0])
                match = re.findall('共有部分面積：(\d+\.*\d*)坪', info.text)
                if match:
                    public_ping = float(match[0])
                match = re.findall(r'類型：(.+?)\n', info.text)
                if match:
                    house_type = match[0]
                match = re.findall(r'屋齡：(.+?)\n', info.text)
                if match:
                    house_age = match[0]
                    match = re.search(r'(\d+)年', house_age)
                    if match:
                        year = int(match.group(1))
                    else:
                        year = 0
                    match = re.search(r'(\d+)個月', house_age)
                    if match:
                        month = int(match.group(1))
                    else:
                        month = 0
                    house_age_v = round((year + (month/12)),1)
                # print(info.text)
                match = re.findall(r'格局：(.+?)\n', info.text)
                if match:
                    pattern = match[0].strip()
                    if '加蓋' in pattern:
                        pattern, pattern1 = pattern.split('加蓋：')
                match = re.findall(r'樓層/總樓高：\s*\n*(.+?)\s*\n*本', info.text)
                if match:
                    floors = match[0].strip()
                    floor_web = floors.split('/')[0]
                    match = re.findall('地上(\d+)層', floors)
                    if match:
                        total_floor = int(match[0])
                    if '地下' in floor_web:
                        floor = -1
                    elif '整棟' in floor_web:
                        floor = 1
                    else:
                        match = re.findall('位於(\d+)層', floors)
                        if match:
                            floor = int(match[0])
                match = re.findall(r'本層戶數：(\d+)戶', info.text)
                if match:
                    house_num = match[0]
                match = re.findall(r'座向：(.+?)\n', info.text)
                if match:
                    blockto = match[0].strip()
                    if not blockto:
                        blockto = ''
                match = re.findall(r'管理方式：(.+?)\n', info.text)
                if match:
                    manage_type = match[0].strip()
                    if not manage_type:
                        manage_type = ''
                match = re.findall(r'管理費：(.+?)\n', info.text)
                if match:
                    manage_fee = match[0].strip()
                    if not manage_fee:
                        manage_fee = ''
                match = re.findall(r'車位：\s*\n*(.+?)\n', info.text)
                if match:
                    parking_type = match[0].strip()
                match = re.findall(r'建物用途：(.+?)\n', info.text)
                if match:
                    situation = match[0].strip()
                match = re.findall(r'經紀人簽章：(.+?)\n', info.text)
                if match:
                    contact_man = match[0].split('-')[0]
                feature = ''
                for tag in soup.find('div',{'class':'obj_big_detail2 obj_big_details col-sm-12'}):
                    # 內容分行
                    contents = tag.get_text(separator="\n").splitlines()
                    # 跌代每一行
                    for line in contents:
                        feature += (line.strip())
                        feature += "\n"
                feature = feature.strip()
                img = soup.find('div',{'class':'obj_big_detail4 obj_big_details col-sm-12'})
                if img:
                    img_url = 'https://www.nra.com.tw/' + img.find('img')['src'].lstrip('..')
                else:
                    img_url = ''
                is_branch = soup.select_one('body > div.inside_cont_wrap > div.obj_img_wrap.clearfix > div.img_wrap_right > div.obj_app_box1 > div.obj_app_box12.clearfix > div.obj_app_left2 > span:nth-child(1) > a')
                branch = is_branch.text if is_branch else ''
                is_company = soup.select_one('body > div.inside_cont_wrap > div.obj_img_wrap.clearfix > div.img_wrap_right > div.obj_app_box1 > div.obj_app_box12.clearfix > div.obj_app_left2 > span:nth-child(2)')
                company = is_company.text if is_company else ''
                is_phone = soup.select_one('body > div.inside_cont_wrap > div.obj_img_wrap.clearfix > div.img_wrap_right > div.obj_app_box1 > div.obj_app_box12.clearfix > div.obj_app_right2 > span')
                phone = is_phone.text if is_phone else ''
                lat, lng = 0,0
                is_map = soup.select_one('body > div.inside_cont_wrap > div.obj_img_wrap.clearfix > div.img_wrap_left > ul > li:nth-child(4) > a')
                if is_map:
                    map = is_map['onclick']
                    match = re.search('(\d+\.\d+),(\d+\.\d+)', map)
                    if match:
                        lat = match.group(1)
                        lng = match.group(2)
                reg_ping = soup.find('span',{'class':'land_size'})
                match = re.findall('土地面積：\s*\n*(\d+\.*\d*)坪', reg_ping.text)
                if match:
                    land_ping = float(match[0])
                if total_ping!= 0:
                        price_ave = round(total/total_ping,2)
                else:
                    if land_ping!=0:
                        price_ave = round(total/land_ping,2)
                    else:
                        price_ave = 0
                brand = '全國不動產'
            
            except requests.exceptions.ProxyError as err:
                self.logger.error(f"Proxy Error processing link {link}: {str(err)}")
                time.sleep(random.uniform(5,10))
                proxy = get_proxies()
                continue

            except Exception as err:
                self.logger.error(f"Error processing link {link}: {str(err)}")
                continue
            
            # 此網站無提供以下資訊
            edge, dark, contact_type, community = '', '', '', ''
            
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