import requests
from bs4 import BeautifulSoup
import re
import time
import pandas as pd
from fake_useragent import UserAgent
import random
from crawler.craw_common import *
requests.packages.urllib3.disable_warnings()

class Cthouse():
    def __init__(self):
        self.name = 'cthouse'
        self.source = '中信房屋'
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
        while True:
            header = {'User-Agent': self.user_agent.random }
            if page % 7 == 0:
                proxy = get_proxies()
            page+=1
            url = f'https://buy.cthouse.com.tw/area/{city}-city/1-ord/page{page}.html'
            time.sleep(random.uniform(10,18))
            for _ in range(3):
                try:
                    res = requests.get(url, headers=header, proxies=proxy, verify=False)
                    res.raise_for_status()
                    break
                
                except requests.exceptions.HTTPError as e:
                    time.sleep(random.uniform(15,30))
                    print(url, 'http失敗，再來一次')
                    proxy = get_proxies()
                    header = {'User-Agent': self.user_agent.random }
                    if _ == 2:
                        self.logger.error(f"Error processing City {city_code} Page {page}: {str(e)}")
                
                except requests.exceptions.ProxyError as e:
                    time.sleep(random.uniform(15,30))
                    print(url, 'proxy失敗，再來一次')
                    proxy = get_proxies()
                    header = {'User-Agent': self.user_agent.random }
                    if _ == 2:
                        self.logger.error(f"Error processing City {city_code} Page {page}: {str(e)}")
            
            try:
                print(url, 'state_code:', res.status_code)
                soup = BeautifulSoup(res.text, 'lxml')
                items = soup.find_all('div',{'class':'item__intro'})
                if not items:
                    # print('沒ㄌ')
                    break
                for item in items:
                    link = 'https://buy.cthouse.com.tw' + item.find('a')['href']
                    subject = item.find('a').text
                    re_price = item.find('span',{"class":'price--real'}).text.replace(',','').rstrip('萬')
                    if '億' in re_price:
                        yi, wan = re_price.split('億')
                        wan = wan.zfill(4)
                        re_price = int(yi+wan)
                    else:
                        re_price = int(float(re_price))
                    source_id = re.search(r'house/(\d+).html', link).group(1)
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
        list_obj =is_new(self, city_code)
        data_list = []
        for obj in list_obj:
            try:
                subject = obj['subject']
                link = obj['link']
                source_id = obj['source_id']
                total = obj['re_price']
                city = obj['city']
                print(subject, link)
                time.sleep(random.uniform(11,20))
                if len(data_list) % 3 == 0:
                    header = {'User-Agent': self.user_agent.random }
                if len(data_list) % 5 == 0:
                    proxy = get_proxies()
                res = requests.get(link, headers=header, proxies=proxy, verify=False)
                res.raise_for_status()
                soup = BeautifulSoup(res.text, 'lxml')
                is_add = soup.find('p',{'class':'house__add'})
                address, area, road = '', '', ''
                if is_add:
                    address = is_add.text.replace('台','臺')
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
                if area == '':
                    is_area = soup.select_one('#body > div.pcShow.breadcrumb > div > li > a:nth-child(10) > span')
                    if is_area:
                        area = is_area.text
                is_feature = soup.find('div',{'class':'detail__foreword'})
                feature = is_feature.text.strip() if is_feature else ''
                is_detail = soup.find('div',{'class':'detail__type'})
                match = re.findall('用途：(.+)\n', is_detail.text)
                situation = match[0] if match else ''
                match = re.findall('格局：(.+)\n', is_detail.text)
                pattern = match[0] if match else ''
                match = re.findall('建物登記：(.+?)坪', is_detail.text)
                total_ping = float(match[0].replace(',','')) if match else 0
                match = re.findall('主建物：(.+)坪\n', is_detail.text)
                building_ping = float(match[0]) if match else 0
                match = re.findall('土地坪數：(.+)坪\n', is_detail.text)
                land_ping = float(match[0].replace(',','')) if match else 0
                floor_web, floor, total_floor = '',0,0
                match = re.findall('所在樓層：(.+)\n', is_detail.text)
                if match:
                    floor_detail = match[0]
                    floor_web, total_floor = floor_detail.split('/共')
                    floor_web = floor_web.rstrip('樓')
                    total_floors = total_floor.rstrip('樓')
                    if total_floors == '-':
                        total_floor = 0
                    else:
                        total_floor = int(total_floors)
                    if '地下' in floor_web:
                        floor = -1
                    else:
                        if '~' in floor_web:
                            floor = int(floor_web.split('~')[0])
                        elif floor_web == '-':
                            floor_web, floor = 0, 0
                        else:
                            floor = int(floor_web)
                match = re.findall('車位描述：(.+)\n', is_detail.text)
                parking_type = match[0] if match else ''
                match = re.findall('座向：(.+)\n', is_detail.text)
                blockto = match[0] if match else ''
                match = re.findall('房屋管理費：(.+)\n', is_detail.text)
                manage_fee = match[0] if match else ''
                house_age, house_age_v = '', 0
                match = re.findall('屋齡：(.+)\n', is_detail.text)
                if match:
                    house_age = match[0].strip()
                    if house_age == '--':
                        house_age, house_age_v = '',0
                    elif '預售' in house_age :
                        house_age_v = 0
                    elif '年' in house_age:
                        house_age = house_age.split('年')[0]
                        house_age_v = house_age
                pings = soup.select_one('#popup4 > div > div.con__wrap > div > div')
                if pings:
                    match = re.findall('>附屬建物小計：(.+?) 坪', pings.text)
                    if match:
                        att_ping = float(match[0])
                    else:
                        att_ping = 0
                    match = re.findall('>共同使用小計：(.+?) 坪', pings.text)
                    if match:
                        public_ping = float(match[0])
                    else:
                        public_ping = 0
                title = soup.select_one('head > title').text
                try:
                    house_type = re.findall('(.+?)出售', title)[0].strip()
                except:
                    house_type = ''
                #print(total_ping, building_ping)
                img = soup.select_one('#houseMorePhoto > div > img:nth-child(1)')
                if img:
                    img_url = img.get_attribute_list('src')[0]
                else:
                    img_url = ''
                if total_ping!= 0:
                    price_ave = round(total/total_ping,2)
                else:
                    if land_ping!=0:
                        price_ave = round(total/land_ping,2)
                    else:
                        price_ave = 0
                scripts = soup.find_all('script')
                try:
                    lng = round(float(re.findall(r'longitude =\s*([\d.]+)', scripts[17].text)[0]),7)
                    lat = round(float(re.findall(r'latitude =\s*([\d.]+)', scripts[17].text)[0]),7)
                except:
                    lat,lng = 0,0
                is_phone = soup.select_one('#body > div.house > div > div.house__mainRow > div.mainRow__right > div.house__call > div.call__top > div > div.txt3 > a')
                if is_phone:
                    phone = is_phone.get_text(strip=True)
                else:
                    phone = ''
                is_branch = soup.select_one('#body > div.house > div > div.house__mainRow > div.mainRow__right > div.house__call > div.call__btm > div.btm__txt > div.txt4 > a')
                if is_branch:
                    branch = is_branch.get_text()
                else:
                    branch = ''
                is_company = soup.select_one('#body > div.house > div > div.house__mainRow > div.mainRow__right > div.house__call > div.call__btm > div.btm__txt > div.txt5')
                if is_company:
                    company = is_company.get_text()
                else:
                    company = ''
                contact_type = '房仲'
                brand = '中信房屋'
            
            except requests.exceptions.ProxyError as err:
                self.logger.error(f"Proxy Error processing link {link}: {str(err)}")
                time.sleep(random.uniform(15,20))
                proxy = get_proxies()
                header = {'User-Agent': self.user_agent.random }
                
            except requests.exceptions.HTTPError as err:
                self.logger.error(f"HTTP Error processing link {link}: {str(err)}")
                time.sleep(random.uniform(30,60))
                proxy = get_proxies()
                header = {'User-Agent': self.user_agent.random }

            except Exception as e:
                self.logger.error(f"Error processing link {link}: {str(e)}")
                continue
            
            else:
                # 中信房屋無提供以下資料
                pattern1, house_num,  manage_type, edge, dark, community, contact_man = '', '', '', '', '', '', ''
                
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