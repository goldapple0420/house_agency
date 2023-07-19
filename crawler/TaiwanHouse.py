from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from fake_useragent import UserAgent 
import time,random
import requests
from bs4 import BeautifulSoup as bs
import re
import pandas as pd
from crawler.craw_common import (to_sqlite, WriteLogger)
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re,time,json
from fake_useragent import UserAgent
import random
from crawler.craw_common import *

def turn_float(object):
                match = re.search(r'\d+', object)
                final = float(match.group())
                return final

class TaiwanHouse():
    def __init__(self):
        self.logger = WriteLogger().getLoggers()
        self.name = 'TaiwanHouse'
        self.source = '台灣房屋'

    def get_items(self, city_code):
        data_list = []
        list_obj = []
        all_city_dict = {"A": "台北市","B": "台中市","C": "基隆市","D": "台南市","E": "高雄市","F": "新北市","G": "宜蘭縣","H": "桃園市","I": "嘉義市","J": "新竹縣","K": "苗栗縣","M": "南投縣","N": "彰化縣","O": "新竹市","P": "雲林縣","Q": "嘉義縣","T": "屏東縣","U": "花蓮縣","V": "台東縣","X":"澎湖縣","W":"金門縣","Z":"連江縣"}
        service = Service(executable_path='/home/yeshome/house_agency/chromedriver/chromedriver')
        house_href = []
        ua = UserAgent()
        options = webdriver.ChromeOptions()
        # options.add_argument('--headless') 
        options.add_argument("user-agent=" + ua.chrome)
        driver = webdriver.Chrome(service=service, options=options)
        
        for code in city_code:
            if code == 'Z':
                continue
            url = "https://www.twhg.com.tw/object_list-A.php?city={}&kid=9".format(all_city_dict[code])
            driver.get(url)

            time.sleep(random.uniform(5, 8))

            items = driver.find_elements (By.XPATH, "/html/body/div[4]/div[1]/div/div/div/div[8]/div[3]/ul/li/div/div[3]/a")
            obj = 1
            re_price = driver.find_element (By.XPATH,'//*[@id="uLiDivSelt"]/li[{}]/div/div[8]/span'.format(obj)).text
            match_re_price = re.search(r'\d+', re_price)
            for i in items:
                house_href.append(i.get_attribute('href'))
                obj_data = {
                            "source_id" : driver.find_element (By.XPATH,'//*[@id="uLiDivSelt"]/li[{}]/div/div[2]'.format(obj)).text,
                            "subject" : driver.find_element (By.XPATH,'//*[@id="uLiDivSelt"]/li[{}]/div/div[3]/a'.format(obj)).text,
                            "link" : i.get_attribute('href'),
                            "city" : all_city_dict[code],
                            "re_price" : float(match_re_price.group()),
                            "insert_time" : time.strftime("%Y-%m-%d %H:%M:%S")
                            }
                obj += 1
                list_obj.append(obj_data)

            for_page = 2
            while for_page <= 7:
                    try:
                        next_page = driver.find_element (By.XPATH,"/html/body/div[4]/div[1]/div/div/div/div[9]/li[{}]/a".format(for_page))
                        next_page.click()
                        items = driver.find_elements (By.XPATH, "/html/body/div[4]/div[1]/div/div/div/div[8]/div[3]/ul/li/div/div[3]/a")
                        obj = 1
                        re_price = driver.find_element (By.XPATH,'//*[@id="uLiDivSelt"]/li[{}]/div/div[8]/span'.format(obj)).text
                        match_re_price = re.search(r'\d+', re_price)
                        for i in items:
                            house_href.append(i.get_attribute('href'))
                            obj_data = {
                                        "source_id" : driver.find_element (By.XPATH,'//*[@id="uLiDivSelt"]/li[{}]/div/div[2]'.format(obj)).text,
                                        "subject" : driver.find_element (By.XPATH,'//*[@id="uLiDivSelt"]/li[{}]/div/div[3]/a'.format(obj)).text,
                                        "link" : i.get_attribute('href'),
                                        "city" : all_city_dict[code],
                                        "re_price" : float(match_re_price.group()),
                                        "insert_time" : time.strftime("%Y-%m-%d %H:%M:%S")
                                        }
                            obj += 1
                            list_obj.append(obj_data)
                        time.sleep(5)
                        for_page += 1
                        time.sleep(2)
                    except:
                        break

            while for_page:
                    try:
                        next_page = driver.find_element (By.XPATH,"/html/body/div[4]/div[1]/div/div/div/div[9]/li[7]/a")
                        next_page.click()
                        time.sleep(5)
                        items = driver.find_elements (By.XPATH, "/html/body/div[4]/div[1]/div/div/div/div[8]/div[3]/ul/li/div/div[3]/a")
                        obj = 1
                        re_price = driver.find_element (By.XPATH,'//*[@id="uLiDivSelt"]/li[{}]/div/div[8]/span'.format(obj)).text
                        match_re_price = re.search(r'\d+', re_price)
                        for i in items:
                            house_href.append(i.get_attribute('href'))
                            obj_data = {
                                        "source_id" : driver.find_element (By.XPATH,'//*[@id="uLiDivSelt"]/li[{}]/div/div[2]'.format(obj)).text,
                                        "subject" : driver.find_element (By.XPATH,'//*[@id="uLiDivSelt"]/li[{}]/div/div[3]/a'.format(obj)).text,
                                        "link" : i.get_attribute('href'),
                                        "city" : all_city_dict[code],
                                        "re_price" : float(match_re_price.group()),
                                        "insert_time" : time.strftime("%Y-%m-%d %H:%M:%S")
                                        }
                            obj += 1
                            list_obj.append(obj_data)
                        time.sleep(2)
                        for_page += 1
                    except:
                        break
            if list_obj:
                to_sqlite(self, list_obj)
            driver.quit()

            for h in house_href:
                time.sleep(1)
                try:
                    house = requests.get(h)
                    soup = bs(house.text, 'lxml')
                    house_ID = soup.find(class_='ta_no')
                    info_much = soup.find(class_='desc-text').contents[1]
                    info = []
                    house_info = {}
                    house_info['網址'] = h
                    house_info['序號'] = house_ID.text
                    num = 0
                    for i in info_much:
                        if i.text.strip():
                            info.append(i.text.strip().replace("\n", "").replace("相似坪數", "").replace("附近行情", "").replace("\xa0", "").replace("\u3000", ""))
                            try:
                                key, value = info[num].split('：')
                                house_info[key] = value.strip()
                            except:
                                    try:
                                        pin = info[num].split('‧ ')
                                        for a in pin:
                                            if a:
                                                key, value = a.split('：')
                                                house_info[key] = value.strip()
                                    except:
                                        try:
                                            pin = info[num].replace('‧ ', '')
                                            pins = pin.split('坪')
                                            for a in pins:
                                                if a:
                                                    key, value = a.split('：')
                                                    house_info[key] = value.strip()
                                        except:
                                            pass
                            num = num+1
                except:
                    pass


                try:
                    pattern = r"(\S+[市縣])(\S+[市區鎮鄉])(\S+)"
                    match = re.match(pattern, house_info['地址'])
                    house_info['城市'] = match.group(1)
                    house_info['行政區'] = match.group(2)
                    house_info['路段'] = match.group(3)
                except:
                    pass


                try:
                    house_info['銷售員'] = soup.find(class_='R-info').contents[3].text
                    house_info['銷售手機'] = soup.find(class_='phone').text
                    house_info['銷售分店'] = soup.find(class_='storeName').text
                    house_info['銷售市話'] = soup.find(class_='storePhone').text
                    house_info['銷售店址'] = soup.find(class_='storeAdr').text
                except:
                    try:
                        house_info['銷售分店'] = soup.find(class_='storeName').text
                        house_info['銷售市話'] = soup.find(class_='storePhone').text
                        house_info['銷售店址'] = soup.find(class_='storeAdr').text
                    except:
                        pass


                try:
                    extra = soup.find(class_='spac-houseTxt')
                    extra_info = []
                    for i in extra:
                        if i.text:
                            extra_info.append(i.text.replace('\n', '。'))
                    house_info['特色'] = ''.join(extra_info)
                except:
                    pass

                try:
                    info_year = soup.find(class_='desc-text').contents[6].text.strip()
                    floor = soup.find(class_='desc-text').contents[8].text.strip()
                    if '屋齡' in info_year:
                        house_info['屋齡'] = info_year.split('：')[1]
                    if '樓層' in floor:
                        house_info['樓層'] = floor.split('：')[1].split('/')[0]
                        house_info['總樓層'] = floor.split('：')[1].split('/')[1]
                except:
                    pass

                data = {}

                data["source"]='台灣房屋'
                try:
                    data["source_id"]=house_info['序號']
                except:
                    continue
                data["subject"]=soup.find(class_='name').text
                try:
                    data["city"]=house_info['城市'].replace('台', '臺')
                except:
                    data["city"]=''
                try:
                    data["area"]=house_info['行政區'].replace('台', '臺')
                except:
                    data["area"]=''
                try:
                    data["road"]=house_info['路段']
                except:
                    data["road"]=''
                data["address"]=house_info['地址'].replace('台', '臺')
                try:
                    data["situation"]=house_info['類型']
                except:
                    data["situation"]=''
                data["total"]=turn_float(house_info['總價'])
                try:
                    data["price_ave"]=house_info['單價']
                except:
                    data["price_ave"]=''
                try:
                    data["feature"]=house_info['特色']
                except:
                    data["feature"]=''
                try:
                    data["pattern"]=house_info['格局']
                except:
                    data["pattern"]=''
                data["pattern1"]=''
                try:
                    data["total_ping"]=turn_float(house_info['建物坪數'])
                except:
                    data["total_ping"]=''
                try:
                    data["building_ping"]=turn_float(house_info['主建物'])
                except:
                    data["building_ping"]=''
                try:
                    data["public_ping"]=turn_float(house_info['共同使用'])
                except:
                    data["public_ping"]=''
                try:
                    data["att_ping"]=turn_float(house_info['附屬建物小計'])
                except:
                    data["att_ping"]=''
                try:
                    data["land_ping"]=turn_float(house_info['屋齡'])
                except:
                    data["land_ping"]=''
                try:
                    data["house_age"]=turn_float(house_info['屋齡'])
                except:
                    data["house_age"]=''
                try:
                    data["house_age_v"]=turn_float(house_info['屋齡'])
                except:
                    data["house_age_v"]=''
                try:
                    data["floor_web"]=turn_float(house_info['樓層'])
                except:
                    data["floor_web"]=''
                try:
                    data["floor"]=turn_float(house_info['樓層'])
                except:
                    data["floor"]=''
                try:
                    data["total_floor"]=turn_float(house_info['總樓層'])
                except:
                    data["total_floor"]=''
                data["house_num"]=''
                data["blockto"]=''
                try:
                    data["house_type"]=house_info['類型']
                except:
                    data["house_type"]=''
                data["manage_type"]=''
                try:
                    data["manage_fee"]=house_info['建物管理費']
                except:
                    data["manage_fee"]=''
                data["edge"]=''
                data["dark"]=''
                data["parking_type"]=''
                try:
                    data["img_url"]=soup.find("img", xpath='//*[@id="arch05"]/section[2]/div/ul/li[1]/div/a').get("src")
                except:
                    data["img_url"]=''
                data["contact"]=''
                try:
                    data["contact_man"]=house_info['銷售員']
                except:
                    data["contact_man"]=''
                try:
                    data["phone"]=house_info['銷售手機']
                except:
                    data["phone"]=''
                try:
                    data["brand"]=house_info['台灣房屋']
                except:
                    data["brand"]=''
                try:
                    data["branch"]=house_info['銷售分店']
                except:
                    data["branch"]=''
                try:
                    data["company"]=house_info['台灣房屋']
                except:
                    data["company"]=''
                data["price_renew"]=0
                data["insert_time"]=time.strftime("%Y-%m-%d %H:%M:%S")
                data["update_time"]=time.strftime("%Y-%m-%d %H:%M:%S")
                data["community"]=''
                data["mrt"]=''
                data["group_man"]=''
                data["group_key"]=''
                data["group_record"]=''
                data["history"]=''
                data["address_cal"]=''
                data["is_delete"]=0
                data["is_hidden"]=0
                data_list.append(data)
            
                # 如果超過100筆物件，就先寫入資料庫，並清空data_list
                if len(data_list) % 100 == 0:
                    data2sql(data_list, city_code)
                    data_list = []
                    house_info.clear()
                    data.clear()

            data2sql(data_list, city_code)
            # 把物件都寫進資料庫之後，在寫入group_key跟address_cal
            find_group(self, city_code)
            find_possible_addrs(self, city_code)
