from queue import Queue
import threading
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from fake_useragent import UserAgent 
import time,random
import requests
from bs4 import BeautifulSoup as bs
import re
import argparse
from crawler.yungching import Yungching
from crawler.eagle import Eagle
from crawler.century import Century21
from crawler.chumken import Chumken
from crawler.chyi import Chyi
from crawler.cthouse import Cthouse
from crawler.dff import Dff
from crawler.edhouse import Edhouse
from crawler.era import Era
from crawler.etwarm import Etwarm
from crawler.go5333 import Go5333
from crawler.house777 import House777
from crawler.housefun import HouseFun
from crawler.singfujia import SingFuJija
from crawler.hshouse import HShouse
from crawler.houseol import HouseOl
from crawler.snhouse import SNhouse
from crawler.taiching import Taiching
from crawler.utrust import Utrust
from crawler.housemama import HouseMama
from crawler.rakuya import Rakuya
from crawler.myhomes import Myhomes
from crawler.sinyi import Sinyi
from crawler.houseinfo import HouseInfo
from crawler.nra import Nra
from crawler.pacific import Pacific
from crawler.houseweb import HouseWeb
from crawler.house588 import House588
from crawler.yes319 import Yes319
from crawler.great_home import GreatHome
from crawler.hbhousing import HBhouse
from crawler.house591 import House591
from crawler.TaiwanHouse import TaiwanHouse
from crawler.craw_common import is_update

exitFlag = 0

class myThread (threading.Thread):
    def __init__(self, threadID, name, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q
    def run(self):
        print ("Starting " + self.name)
        process_data(self.name, self.q)
        print ("Exiting " + self.name)

def process_data(threadName, q):
    # global counter
    # counter = 0
    while exitFlag == 0:
        queueLock.acquire()
        if not workQueue.empty() and threadName == 'Thread-台灣房屋':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            taiwanHouse = TaiwanHouse()
            taiwanHouse.get_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-永慶房屋':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            yungching = Yungching()
            yungching.get_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-信義房屋':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            sinyi = Sinyi()
            sinyi.get_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-中信房屋':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            cthouse = Cthouse()
            cthouse.get_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-幸福家':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            singfujia = SingFuJija()
            singfujia.get_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-東龍':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            edhosue = Edhouse()
            edhosue.get_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-群義房屋':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            chyi = Chyi()
            chyi.get_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-飛鷹':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            eagle = Eagle()
            eagle.get_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-僑福房屋':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            go5333 = Go5333()
            go5333.get_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-惠雙房屋':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            hshouse = HShouse()
            hshouse.get_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-ERA':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            era = Era()
            era.get_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-南北':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            snhouse = SNhouse()
            snhouse.get_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-台慶':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            taiching = Taiching()
            taiching.get_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-21世紀':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            century21 = Century21()
            century21.get_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-大家房屋':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            great_home = GreatHome()
            great_home.get_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-住商':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            hbhousing = HBhouse()
            hbhousing.get_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-全國':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            nra = Nra()
            # nra.get_items(city_code)
            is_update(nra, city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-樂屋網':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            rakuya = Rakuya()
            rakuya.get_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-好房網':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            housefun = HouseFun()
            housefun.get_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-591':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            house591 = House591()
            house591.get_house_items(city_code)
            house591.get_other_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-yes319':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            yes319 = Yes319()
            yes319.get_house_items(city_code)
            yes319.get_land_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-東森房屋':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            etwarm = Etwarm()
            etwarm.get_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-太平洋房屋':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            pacific = Pacific()
            pacific.get_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-有巢氏房屋':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            utrust = Utrust()
            utrust.get_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-春耕房屋':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            chumken = Chumken()
            chumken.get_land_items(city_code)
            chumken.get_hosue_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-我家網':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            myhomes = Myhomes()
            myhomes.get_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-淘屋網':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            houseweb = HouseWeb()
            houseweb.get_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-愛屋線上':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            houseol = HouseOl()
            houseol.get_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-777':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            house777 = House777()
            house777.get_house_items(city_code)
            house777.get_land_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-大豐富':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            dff= Dff()
            dff.get_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-房屋資訊網':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            houseinfo = HouseInfo()
            houseinfo.get_house_items(city_code)
            houseinfo.get_land_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-房屋媽媽':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            housemama = HouseMama()
            housemama.get_items(city_code)
            q.task_done()
        elif not workQueue.empty() and threadName == 'Thread-588':
            city_code = q.get()
            print("{} processing {}".format(threadName, city_code))
            queueLock.release()
            house588 = House588()
            house588.get_items(city_code)
            q.task_done()
        else:
            queueLock.release()
        time.sleep(1)

threadList = {
    '1':"Thread-台灣房屋",
    '2':"Thread-永慶房屋",
    '3':"Thread-信義房屋",
    '4':"Thread-中信房屋",
    '5':"Thread-幸福家",
    '6':"Thread-東龍",
    '7':"Thread-群義房屋",
    '8':"Thread-飛鷹",
    '9':"Thread-僑福房屋",
    '10':"Thread-惠雙房屋",
    '11':"Thread-ERA",
    '12':"Thread-南北",
    '13':"Thread-台慶",
    '14':"Thread-21世紀",
    '15':"Thread-大家房屋",
    '16':"Thread-住商",
    '17':"Thread-全國",
    '18':"Thread-樂屋網",
    '19':"Thread-好房網",
    '20':"Thread-591",
    '21':"Thread-東森房屋",
    '22':"Thread-太平洋房屋",
    '23':"Thread-有巢氏房屋",
    '24':"Thread-春耕房屋",
    '25':"Thread-我家網",
    '26':"Thread-淘屋網",
    '27':"Thread-愛屋線上",
    '28':"Thread-777",
    '29':"Thread-大豐富",
    '30':"Thread-房屋資訊網",
    '31':"Thread-房屋媽媽",
    '33':"Thread-588",
    '33':'Thread-yes319'
    }

city_codes = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "M", "N", "O", "P", "Q", "T", "U", "V", "X", "W", "Z"]


def createArgumentParser():
    # 創建參數解析器
    parser = argparse.ArgumentParser(description='房仲網爬取程式')

    parser.add_argument('-s', help='指定要爬取的房仲網站：數字1～33，一定要填！', type=str, dest='AGENCY_SOURCE', required=True)
    parser.add_argument('-c', help='指定要爬取的縣市代碼：大寫英文，預設為ALL', type=str, dest='CITY', default=','.join(city_codes))
    
    args = parser.parse_args()
    return args

args = createArgumentParser()
sources = []
for source in args.AGENCY_SOURCE.split(','):
    sources.append(threadList[source])
num = len(sources)

queueLock = threading.Lock()
workQueue = Queue(num)
threads = []
threadID = 1

for tName in sources:
    thread = myThread(threadID, tName, workQueue)
    thread.start()
    threads.append(thread)
    threadID += 1

time.sleep(1)

for code in args.CITY.split(','):
    queueLock.acquire()
    for i in range(num):
        workQueue.put(code)
    queueLock.release()
    # time.sleep(5)
    # while not counter%num == 0:
    #     pass
    workQueue.join()
    print("{}已跑完".format(code))


while not workQueue.empty():
    pass

exitFlag = 1

for t in threads:
    t.join()
print("Exiting Main Thread")