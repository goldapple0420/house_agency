import requests
from bs4 import BeautifulSoup
import pandas as pd
import time, re, datetime, random
from fake_useragent import UserAgent
from crawler.craw_common import *


# 若網頁上寫3000筆，實際寫入sqlite只有1800筆是正常的喔，因為真的有很多已下架的物件
class Dapuli():
    def __init__(self):
        self.name = 'dapuli'
        self.source = '大埔里房仲網'
        self.user_agent = UserAgent()
        self.logger = WriteLogger().getLoggers()

    def get_items(self, city_code):
        list_obj = []
        city = '南投縣'
        if city_code != 'M': # 只有南投縣物件
            # print(f'City {city_code}無物件')
            return list_obj
        page = 0
        while True:
            page+=1
            url = f'http://www.0492424550.idv.tw/search.php?page={page}'
            header = {'User-Agent' : self.user_agent.random}
            s = requests.session()
            try:
                time.sleep(random.uniform(1,2))
                res = s.get(url, headers=header)
                res.encoding = 'utf-8'
                soup = BeautifulSoup(res.text, 'lxml')
                items = soup.find_all('div',{'class':'search_product_all'})
                if not items:
                    # print(f'City {city_code} 共{len(list_obj)}個物件')
                    break
                print(f'{self.source} {city} P{page} {len(items)}個物件')
                for item in items:
                    link = 'http://www.0492424550.idv.tw/' + item.find('a')['href']
                    link = link.rstrip('&house_type=&build_type=&area_id=1')
                    source_id = re.findall('object_id=(\d+)', link)[0]
                    subject = item.find('div',{'class':'search_text_all'}).find('span').text
                    if ('下架' in subject) or ('賀成交' in subject) : # 有以下架的物件還存在網頁上，直接跳過
                        continue
                    detail = item.find('div',{'class':'search_text_all'}).text
                    try:
                        price = re.findall('總價： (.+)', detail)[0]
                        # 有億一定是總價
                        if '億' in price:
                            yi,wan = price.replace('元','').rstrip('萬').split('億')
                            if '千' in wan:
                                wan = str(float(wan.replace('千',''))*1000)
                            elif '仟' in wan:
                                wan = str(float(wan.replace('仟',''))*1000)
                            else:
                                wan = wan.zfill(4)
                            re_price = int(float(yi+wan))
                        # 沒億可能是總價
                        else:
                            match = re.findall('^(\d+)\.?\d*萬', price)
                            if match:
                                re_price = int(match[0])
                            else:
                                # 不符合的可能是單價，但格式不一
                                if '坪' in price:
                                    match2 = re.findall('(\d+\.?\d*)', price)
                                    if match2:
                                        price_ave = float(match2[0])
                                        ping = float(re.findall('地坪：.?(\d+\.?\d*)', detail)[0])
                                        re_price = int(price_ave * ping)
                        obj_data = {
                                    "source_id" : source_id,
                                    "subject" : subject,
                                    "link" : link,
                                    "city" : city,
                                    "re_price" : int(re_price),
                                    "insert_time" : time.strftime("%Y-%m-%d %H:%M:%S")
                                    }
                        list_obj.append(obj_data)
                    except Exception as e:# 物件有格式混亂的跳過
                        self.logger.error(f"資料格式混亂Error processing City {city_code} Page {page} Link {link}: {str(e)}")
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
        header = {'User-Agent' : self.user_agent.random}
        proxy = get_proxies()
        for obj in list_obj:
            try:
                time.sleep(random.uniform(1,2))
                link = obj['link']
                source_id = obj['source_id']
                subject = obj['subject']
                total = obj['re_price']
                city = obj['city']
                print(subject, link)
                res = requests.get(link, headers=header, proxies=proxy)
                res.encoding = 'utf-8'
                soup = BeautifulSoup(res.text, 'lxml')
                img_url = soup.select_one('#pic_view').find('img')['src']
                if 'nopic' in img_url: # 沒圖片時的圖片網址
                    img_url = ''
                match = re.findall(' > (.+) > ',soup.find('div',{'class':'search_mb'}).text)
                house_type = match[0] if match else ''
                land_ping, total_ping, building_ping, att_ping, public_ping, total = 0,0,0,0,0,0
                address, blockto, pattern, parking_type, manage_type, manage_fee, feature = '', '', '', '', '', '', ''
                house_age, house_age_v, floor_web, floor, total_floor = '', 0, '', 0, 0
                contact_man, phone, company = '', '', ''
                area, road = '', ''
                infos = soup.find('div',{'class':'inside_table'}).find_all('tr')
                for tr in infos:
                    match = re.findall('土地總坪數\n(\d+\.?\d*)坪', tr.text)
                    if match:
                        land_ping = float(match[0])
                    match = re.findall('登記總面積\n(\d+\.?\d*)坪', tr.text)
                    if match:
                        total_ping = float(match[0])
                    match = re.findall('主建坪數\n(\d+\.?\d*)坪', tr.text)
                    if match:
                        building_ping = float(match[0])
                    match = re.findall('附屬坪數\n(\d+\.?\d*)坪', tr.text)
                    if match:
                        att_ping = float(match[0])
                    match = re.findall('共有坪數\n(\d+\.?\d*)坪', tr.text)
                    if match:
                        public_ping = float(match[0])
                    match = re.findall('地址\n(.+)\n', tr.text)
                    if match:
                        address = match[0].replace('台','臺')
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
                    city = obj['city']
                    match = re.search('建物完成日\n(\d+).(\d+).\d+', tr.text)
                    if match:
                        year = int(match.group(1)) + 1911 # 物件提供的是民國年
                        month = int(match.group(2))
                        today = datetime.date.today()
                        year = today.year - year
                        month = today.month - month
                        house_age_v = round(year + (month / 12),1)
                        house_age = str(house_age_v) + '年'
                    match = re.findall('屋齡\n(\d+\.?\d*)年', tr.text)
                    if match:
                        house_age = match[0] + '年'
                        house_age_v = float(match[0])
                    match = re.findall('建物總樓層\n(\d+)樓', tr.text)
                    if match:
                        total_floor = int(match[0])
                    match = re.findall('所在樓層\n(.+樓)', tr.text)
                    if match:
                        floor_web = match[0]
                        if 'B' in floor_web:
                            floor = -1
                        else:
                            match = re.findall('\d+', floor_web)
                            if match:
                                floor = int(match[0])
                    match = re.findall('朝向\n(.+)\n', tr.text)
                    if match:
                        blockto = match[0]
                    match = re.findall('格局\n(.+)\n', tr.text)
                    if match:
                        pattern = match[0]
                    match = re.findall('車位型態\n(.+)\n', tr.text)
                    if match:
                        parking_type = match[0]
                    match = re.findall('管理方式\n(.+)\n', tr.text)
                    if match:
                        manage_type = match[0]
                    match = re.findall('管理費約\n(.+)\n', tr.text)
                    if match:
                        manage_fee = match[0]
                    match = re.findall('物件簡介\n([\S\s]+)', tr.text)
                    if match:
                        feature = match[0].strip()
                    match = re.findall('聯絡人姓名\n(.+)\n', tr.text)
                    if match:
                        contact_man = match[0]
                    match = re.findall('所屬公司名稱\n(.+)\n', tr.text)
                    if match:
                        company = match[0]
                    match = re.findall('公司電話\n(.+)\n', tr.text)
                    if match:
                        phone = match[0]
                price_ave = 0
                # 少數例外是在委託價格寫單價, 抓出單價後用單價計算總價
                if total == 0:
                    match = re.findall('(\d+\.?\d*)/坪萬元', infos[2].text)
                    if match:
                        price_ave = float(match[0])
                        if total_ping!= 0:
                            total = total_ping * price_ave
                        else:
                            if land_ping!=0:
                                total = land_ping * price_ave
                else: # total不是0，就用總價去計算出單價
                    if total_ping!= 0:
                        price_ave = round(total/total_ping,2)
                    else:
                        if land_ping!=0:
                            price_ave = round(total/land_ping,2)
                        else:
                            price_ave = 0
            
            except requests.exceptions.ProxyError as err:
                print(err)
                time.sleep(random.uniform(5,10))
                proxy = get_proxies()
                continue
            
            except Exception as e:
                self.logger.error(f"Error processing link {link}: {str(e)}")
                continue
            
            # 此網頁無提供以下資訊
            situation, pattern1, contact_type, brand, branch, community, house_num, edge, dark, lat, lng = '', '', '', '', '', '', '', '', '',0,0

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

