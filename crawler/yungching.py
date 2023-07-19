import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import re
import time
import json
import random
from fake_useragent import UserAgent
from crawler.craw_common import *


class Yungching:
    def __init__(self):
        self.user_agent = UserAgent()
        self.source = '永慶房屋'
        self.name = 'yungching'
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
        }

    def get_items(self, city_code):
        # 無提供連江縣物件
        if city_code == 'Z':
            return
        city = self.all_city_dict[city_code]
        city_n = city.replace('臺', '台')
        list_obj = []
        page ,last_page = 0,1
        while page < last_page:
            try:
                if page % 10 == 0:
                    proxy = get_proxies()
                page += 1
                url = f'https://buy.yungching.com.tw/region/{city_n}-_c/?od=80&pg={page}'
                header = {'User-Agent': self.user_agent.random}
                time.sleep(random.uniform(3, 11))
                res = requests.get(url, headers=header, proxies=proxy)
                print(f'開始擷取{self.source} {city} P{page} 物件')
                soup = BeautifulSoup(res.text, 'lxml')
                items = soup.find_all('li', class_="m-list-item")
                for item in items:
                    link = 'https://buy.yungching.com.tw' + item.find('a')['href']
                    source_id = re.findall('house/(\d+)',link)[0]
                    subject = item.find('a')['title']
                    re_price = float(item.find('span',{'class':'price-num'}).text.replace(',',''))
                    obj_data = {
                                "source_id" : source_id,
                                "source" : self.source,
                                "subject" : subject,
                                "link" : link,
                                "city" : city,
                                "re_price" : int(re_price),
                                "insert_time" : time.strftime("%Y-%m-%d %H:%M:%S")
                                }
                    list_obj.append(obj_data)
                last_url = soup.find('a',{'ga_label':'buy_page_last'})['href']
                # 已經在最後一頁時，就不會顯示最後一頁的網址
                if last_url:
                    last_page = int(re.findall(r'pg=(\d+)', last_url)[0])
                else:
                    break
            
            except requests.exceptions.ProxyError as err:
                self.logger.error(f"Proxy Error processing {city} Page {page}: {str(err)}")
                time.sleep(random.uniform(15,30))
                proxy = get_proxies()
                continue

            except Exception as e:
                self.logger.error(f"Error processing Source {self.source} City {city_code} Page {page}: {str(e)}")
                continue

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
        header = {'User-Agent': self.user_agent.random }
        for obj in list_obj:
            link = obj['link']
            source_id = obj['source_id']
            subject = obj['subject']
            print(subject, link)
            for _ in range(3):
                time.sleep(random.uniform(1, 5))
                try:
                    default_response = requests.get(link, headers=header, proxies=proxy)
                    if default_response.status_code == 200:
                        break
                except Exception as err:
                    proxy = get_proxies()   
            # 取得cookies，並帶入
            cookies_key1 = 'SEID_G'
            cookies_key2 = 'TRID_G'
            cookies_key3 = 'yawbewkcehc'
            cookies = {
                    cookies_key1: default_response.cookies[cookies_key1],
                    cookies_key2: default_response.cookies[cookies_key2],
                    cookies_key3: default_response.cookies[cookies_key3],
                    }
            try:
                time.sleep(random.uniform(2, 7))
                res = requests.get(link, headers=header, cookies=cookies, proxies=proxy)
                link_soup = BeautifulSoup(res.text, 'lxml')
                scripts = link_soup.select('script')
                text = str(scripts[2])
                total = float(re.findall(r'total_price:\s*"(\d+\.?\d*)"', text)[0])
                total_ping = float(re.findall(r'ping:\s*"(\d+\.?\d*)"', text)[0])
                try: # 可能物件是土地等原因，age為空
                    house_age_v = float(re.findall(r'age:\s*"(\d+\.?\d*)"', text)[0])
                    house_age = str(house_age_v) + '年'
                except:
                    house_age, house_age_v = '', 0
                house_type = re.findall(r',\s+type:\s*"(.*)"', text)[0]
                if not house_type: # 有些純土地，type為空,purpose才會寫土地
                    house_type = re.findall(r'purpose:\s*"(.+)"', text)[0]
                floor  = re.findall(r'floor:\s*"(.*)"', text)[0]
                if floor:
                    floor = int(floor)
                else:
                    floor = 0
                feature = re.findall(r'badge:\s*\[(.*)\]', text)[0]
                is_blockto = re.findall(r'direction:\s*"(.*)"', text)[0]
                if is_blockto:
                    blockto = '朝向' + is_blockto
                else:
                    blockto = ''
                community = re.findall(r',\s+name:\s*"(.*)"', text)[0]
                city = re.findall(r'city:\s*"(.+)"', text)[0]
                area = re.findall(r'district:\s*"(.+)"', text)[0]
                road = re.findall(r'road:\s*"(.*)"', text)[0]
                lng = round(float(re.findall(r'longitude:\s*"([\d.]+)"', text)[0]),7)
                lat = round(float(re.findall(r'latitude:\s*"([\d.]+)"', text)[0]),7)
                address, situation, pattern, pattern1 = '', '', '', ''
                is_address = link_soup.find('div',{'class':'house-info-addr'})
                if is_address:
                    address = is_address.text
                is_situation = link_soup.select_one("body > main > section.m-house-detail-block.detail-data > section.m-house-detail-list.bg-square > ul > li:nth-child(4)")
                if is_situation:
                    situation = is_situation.get_text().split("：")[1]
                is_pattern = link_soup.select_one("body > main > section.m-house-detail-block.detail-data > section.m-house-detail-list.bg-bed > ul > li")
                if is_pattern:
                    pattern = is_pattern.text.strip()
                    if '，' in pattern:
                        pattern1 = pattern.split('，')[1].strip()
                        pattern = pattern.split('，')[0].strip()
                floor_web, total_floor='', 0
                i_file = link_soup.select_one("body > main > section.m-house-detail-block.detail-data > section.m-house-detail-list.bg-other.last > div:nth-child(2) > ul")
                i_file_2 = link_soup.find('div',{'class':'m-house-info-wrap'})
                # 抓樓層
                if i_file:
                    match = re.search(r'(B?\d+~B?\d+/\d+)樓',i_file.get_text(strip=True))
                    if match:
                        floor_web,total_floor = match.group(1).split('/')
                        total_floor = int(total_floor)
                    else:
                        match = re.search(r'(B?\d+~B?\d+/\d+)樓',i_file_2.get_text(strip=True))
                        if match:
                            floor_web,total_floor = match.group(1).split('/')
                            total_floor = int(total_floor)
                # 坪數
                building_ping, public_ping, att_ping, land_ping = 0, 0, 0, 0
                is_land_ping = link_soup.select_one("body > main > section.m-house-detail-block.detail-data > section.m-house-detail-list.bg-square > ul > li:nth-child(1)")
                if is_land_ping:
                    match = re.findall(r"土地坪數：(\d+\.\d+)坪",is_land_ping.text)
                    if match:
                        land_ping = float(match[0])
                ping = link_soup.select_one("body > main > section.m-house-detail-block.detail-data > section.m-house-detail-list.bg-square > ul > li.right")
                if ping:
                    match = re.findall(r"主建物小計：(\d+\.\d+)坪",ping.text)
                    if match:
                        building_ping = float(match[0])
                    match = re.findall(r"共同使用小計：(\d+\.\d+)坪",ping.text)
                    if match:
                        public_ping = float(match[0])
                    match = re.findall(r"附屬建物小計：(\d+\.\d+)坪",ping.text)
                    if match:
                        att_ping = float(match[0])
                # 自行計算每坪單價，沒有總坪數就用地坪去算
                if total_ping!=0:
                    price_ave = round((total/total_ping),2)
                else:
                    if house_type == '土地':
                        if land_ping:
                            price_ave = round((total/land_ping),2)
                        else:
                            print('此土地沒有提供土地坪數')
                    else:
                        price_ave = 0
                # 其他資訊
                manage_fee, manage_type = '', ''
                info_detail_right = link_soup.find_all('div',{'class':'m-house-detail-ins'})
                if info_detail_right:
                    match = re.search(r"建物管理費：(.+)",info_detail_right[1].text.strip())
                    if match:
                        manage_fee = match.group(1).rstrip('(備註)')
                        try: # 少數只有寫建物管理費，沒寫管理方式
                            if '管理' in info_detail_right[1].text.strip().split('\n')[1]:
                                manage_type = info_detail_right[1].text.strip().split('\n')[1]
                        except:
                            pass
                park = link_soup.find('section',{'class':'m-house-detail-list bg-car'})
                if park:
                    parking_type = park.text.strip().lstrip('車位\n').rstrip('(備註)')
                else:
                    parking_type = ''
                try:
                    img_url = 'https:' + link_soup.find('img',{'class':'carousel-main-photo'})['src']
                except:
                    img_url = ''
                try:
                    phone = link_soup.select_one("body > main > section.m-house-infomation.true-value > div.m-house-infos.right > div.m-info-tel > em").get_text()
                except:
                    phone = ''
                # 房仲資料
                url = "https://buy.yungching.com.tw/ws/GetShopInfoByCaseSID"
                params = {'sid':source_id}
                time.sleep(random.uniform(2, 5))
                res = requests.get(url,params=params,headers=header,cookies=cookies,proxies=proxy)
                html = res.json()
                contact_man = html['ShopInfo']['leadername']
                brand = html['ShopInfo']['hqname']
                branch = html['ShopInfo']['shortshopname']
                company = html['ShopInfo']['companyname']
                if not contact_man:
                    contact_man = ''
                if not brand:
                    brand = ''
                if not branch:
                    branch = ''
                if not company:
                    company = ''
                
            except requests.exceptions.ProxyError as err:
                self.logger.error(f"Proxy Error processing link {link}: {str(err)}")
                time.sleep(random.uniform(15,30))
                proxy = get_proxies()
                continue

            except Exception as e:
                self.logger.error(f"Error processing link {link}: {str(e)}")
                continue
            
            # 此網頁無提供以下資訊
            edge, dark, house_num, contact_type = '', '', '', ''

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
            if len(data_list) % 20 == 0:
                data2sql(data_list, city_code)
                data_list = []

        data2sql(data_list, city_code)
        # 把物件都寫進資料庫之後，再寫入group_key跟address_cal
        find_group(self, city_code)
        find_possible_addrs(self, city_code)