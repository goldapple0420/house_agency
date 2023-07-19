from dataclasses import dataclass
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re, time
import random
from fake_useragent import UserAgent
from crawler.craw_common import  *

class Rakuya():
    def __init__(self):
        self.name = 'rakuya'
        self.source = '樂屋網'
        self.user_agent = UserAgent()
        self.logger = WriteLogger().getLoggers()
        # 金門跟連江是一起的
        self.all_city_dict = {
        "A": {"city_c": "臺北市", "city_e": "0"},
        "B": {"city_c": "臺中市", "city_e": "8"},
        "C": {"city_c": "基隆市", "city_e": "1"},
        "D": {"city_c": "臺南市", "city_e": "14"},
        "E": {"city_c": "高雄市", "city_e": "15"},
        "F": {"city_c": "新北市", "city_e": "2"},
        "G": {"city_c": "宜蘭縣", "city_e": "3"},
        "H": {"city_c": "桃園市", "city_e": "4"},
        "I": {"city_c": "嘉義市", "city_e": "12"},
        "J": {"city_c": "新竹縣", "city_e": "6"},
        "K": {"city_c": "苗栗縣", "city_e": "7"},
        "M": {"city_c": "南投縣", "city_e": "10"},
        "N": {"city_c": "彰化縣", "city_e": "9"},
        "O": {"city_c": "新竹市", "city_e": "5"},
        "P": {"city_c": "雲林縣", "city_e": "11"},
        "Q": {"city_c": "嘉義縣", "city_e": "13"},
        "T": {"city_c": "屏東縣", "city_e": "17"},
        "U": {"city_c": "花蓮縣", "city_e": "19"},
        "V": {"city_c": "臺東縣", "city_e": "18"},
        "X": {"city_c": "澎湖縣", "city_e": "16"},
        "W": {"city_c": "金門連江", "city_e": "20"},
        "Z": {"city_c": "金門連江", "city_e": "20"}
        }

    def get_items(self, city_code):
        list_obj = []
        city_n = self.all_city_dict[city_code]['city_e']
        city = self.all_city_dict[city_code]['city_c']
        proxy = get_proxies()
        page, last_page = 0,1
        while page<last_page:
            page+=1
            url = f'https://www.rakuya.com.tw/sell/result?city={city_n}&sort=11&browsed=0&page={page}'
            header = {'User-Agent': self.user_agent.random }
            try:
                res = requests.get(url, headers=header, proxies=proxy)
                print(f'開始擷取{self.source} {city} P{page} 物件')
                soup = BeautifulSoup(res.text, 'lxml')
                items = soup.find_all('a',{'target':'_blank'})
                for item in items:
                    if 'sell_item/info' in item['href']:
                        link = item['href']
                        subject = item.find('h2').text
                        if '測試物件' in subject:
                            continue
                        source_id = re.findall('ehid=(.+?)&', link)[0]
                        re_price = int(float(item.find('span',{'class':'info__price--total'}).text.rstrip('萬')))
                        # 金門連江的物件放在一起，所以要分辨是金門還是連江
                        if city == '金門連江':
                            area = item.find('span',{'class':'info__geo--area'}).text
                            # 連江縣只有南竿 北竿 莒光 東引
                            if ('竿' in area) or ('東引' in area) or ('莒光' in area):
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
                pages = soup.find('p',{'class':'pages'})
                if pages:
                    last_page = int(re.findall(r'/\s*(\d+)\s*頁', pages.text)[0])

            except requests.exceptions.ProxyError as err:
                self.logger.error(f"Proxy Error processing {city} Page{page}: {str(err)}")
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
        data_list  =[]
        list_obj = is_new(self, city_code)
        for obj in list_obj:
            if len(data_list) % 20 == 0:
                proxy = get_proxies()
            subject = obj['subject']
            link = obj['link']
            source_id = obj['source_id']
            city = obj['city']
            total = obj['re_price']
            header = {'user-agent' : self.user_agent.random}
            for _ in range(3):
                try:
                    print(subject, link)
                    time.sleep(random.randint(3, 10))
                    res = requests.get(link, headers=header, proxies=proxy)
                    soup = BeautifulSoup(res.text, 'lxml')
                    address = soup.find('h1',{'class':'txt__address'}).text.strip()
                    break
                except:
                    print(f'{self.source}ID:{source_id} 連線失敗，重新連線中，請等待15~30秒...')
                    time.sleep(random.randint(15, 30))
                    proxy = get_proxies()
                    header = {'user-agent' : self.user_agent.random}
            try:
                address = address.split('/')[0].replace('台','臺')
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
                img = soup.select_one('#slideshow')
                if img:
                    try:
                        img_url = img.find('img')['data-lazy']
                    except:
                        img_url ='Err'
                else:
                    img_url = ''
                house_type, pattern, situation, blockto, parking_type, manage_type, manage_fee, community = '', '', '', '', '', '', '', ''
                floor_web, floor, total_floor, house_age, house_age_v = '', 0, 0, '', 0
                total_ping, building_ping, att_ping, public_ping, land_ping = 0,0,0,0,0
                info = soup.find('div',{'class':'block__info-sub'})
                details = info.find_all('li')
                for detail in details:
                    if detail.find('span',{'class':'list__label'}).text == '類型':
                        house_type = detail.find('span',{'class':'list__content'}).text
                    elif detail.find('span',{'class':'list__label'}).text == '社區':
                        community = detail.find('span',{'class':'list__content'}).text.strip()
                    elif detail.find('span',{'class':'list__label'}).text == '格局':
                        pattern = detail.find('span',{'class':'list__content'}).text
                    elif detail.find('span',{'class':'list__label'}).text == '樓層/樓高':
                        floors = detail.find('span',{'class':'list__content'}).text
                        if floors == '--':
                            floor_web, floor, total_floor = '', 0, 0
                        elif floors == '頂樓加蓋':
                            floor_web, floor, total_floor = '', 0, 0
                        else:
                            floor_web = floors.split('/')[0]
                            if '/' in floors:
                                total_floor = int(floors.split('/')[1].rstrip('樓'))
                            else:
                                total_floor = int(re.findall('\d+', floors)[-1])
                            if 'B' in floor_web:
                                floor = -1
                            elif '整棟' in floor_web:
                                floor = 1
                            else:
                                match = re.findall('\d+', floor_web)
                                if match:
                                    floor = int(match[0])
                    elif detail.find('span',{'class':'list__label'}).text == '法定用途':
                        situation = detail.find('span',{'class':'list__content'}).text
                    elif detail.find('span',{'class':'list__label'}).text == '屋齡':
                        house_age = detail.find('span',{'class':'list__content'}).text
                        match = re.findall('\d+\.*\d*', house_age)
                        if match:
                            house_age_v = float(match[0])
                    elif detail.find('span',{'class':'list__label'}).text == '朝向':
                        blockto = detail.find('span',{'class':'list__content'}).text
                    elif detail.find('span',{'class':'list__label'}).text == '車位':
                        parking_type = detail.find('span',{'class':'list__content'}).text
                        if parking_type == '-':
                            parking_type = ''
                    elif detail.find('span',{'class':'list__label'}).text == '管理方式':
                        manage_type = detail.find('span',{'class':'list__content'}).text
                    elif detail.find('span',{'class':'list__label'}).text == '管理費':
                        manage_fee = detail.find('span',{'class':'list__content'}).text
                    elif detail.find('span',{'class':'list__label'}).text == '建物登記':
                        total_ping = float(detail.find('span',{'class':'list__content'}).text.rstrip('坪'))
                    elif detail.find('span',{'class':'list__label'}).text == '主建物':
                        building_ping = float(detail.find('span',{'class':'list__content'}).text.rstrip('坪'))
                    elif detail.find('span',{'class':'list__label'}).text == '土地登記':
                        land_ping = float(detail.find('span',{'class':'list__content'}).text.rstrip('坪'))
                    elif detail.find('span',{'class':'list__label'}).text == '附屬建物':
                        att_ping = float(detail.find('span',{'class':'list__content'}).text.rstrip('坪'))
                    elif detail.find('span',{'class':'list__label'}).text == '公共設施':
                        public_ping = float(detail.find('span',{'class':'list__content'}).text.rstrip('坪'))
                feature = ''
                features = soup.select_one('body > div.container > div.main-body > div.main-content > div.block__info-detail > div')
                if features:
                    p_tags = features.find_all('p')
                    for tag in p_tags:
                        if tag.text.strip() != "":
                            feature += tag.text.strip()
                            feature += "\n"
                feature = feature.strip()
                is_man = soup.find('span',{'class':'name'})
                if is_man:
                    contact_man = is_man.text
                else:
                    contact_man = ''
                is_agent = soup.find('span',{'class':'tag__identity'})
                if is_agent:
                    contact_type = is_agent.text
                    if contact_type == '營業員/經紀人':
                        contact_type = '房仲'
                    elif contact_type == '屋主自售':
                        contact_type = '屋主'
                else:
                    contact_type = ''
                is_phone = soup.find('div',{'class':'phone'})
                if is_phone:
                    phone = is_phone.text.replace('掃碼撥打電話', '').strip()
                else:
                    phone = ''
                contact_info = soup.find('div',{'class':'info__group'})
                if contact_info:
                    is_shop = contact_info.find('h2')
                    if is_shop:
                        brand = is_shop.text.split('-')[0].strip()
                        branch = is_shop.text.split('-')[1].replace('[加盟]','').strip()
                    else:
                        brand, branch = '', ''
                    is_company = contact_info.find_all('p')
                    if len(is_company) > 1:
                        company = is_company[1].text
                    else:
                        company = ''
                if contact_type == '屋主':
                    company = ''
                lat, lng = 0,0
                map = soup.select_one('#embedMap')
                if map:
                    map_url = map['src']
                    match = re.search('q=(\d+\.\d+),(\d+\.\d+)', map_url)
                    if match:
                        lat = round(float(match.group(1)),7)
                        lng = round(float(match.group(2)),7)
                if total_ping!= 0:
                    price_ave = round(total/total_ping,2)
                else:
                    if land_ping!=0:
                        price_ave = round(total/land_ping,2)
                    else:
                        price_ave = 0
            
            except AttributeError as err:
                self.logger.error(f"AttributeError processing link {link}: {str(err)}")
                continue

            except Exception as err:
                self.logger.error(f"Error processing link {link}: {str(err)}")
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