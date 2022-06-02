import requests
import csv
import sys
import os
import mysql.connector
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import QObject,QThread,pyqtSignal
from PyQt5.QtWidgets import *
import CompanyVietStock
from concurrent.futures import ThreadPoolExecutor
import datetime


username = "root"
password = "admin"

today = datetime.date.today()
year = today.strftime("%Y")
month = today.strftime("%m")
day = today.strftime("%d")

sql = "INSERT INTO transaction (Source, CompanyID, Trading_Date, Open_Price, Close_Price, Highest, Lowest," \
      "Price_Revised, Diff_Price, Diff_Percent_Price, Matching_Volume, Matching_Value," \
      "Put_Through_All_Volume, Put_Through_All_Value) " \
      "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

def setUsernameAndPassword(user,passw):
    global username
    username = user
    global password
    password = passw

def insert_sql(sql, all_data):
    db = mysql.connector.connect(
        user=f"{username}",
        password=f"{password}",
        database="vietstock2"
    )
    cursor = db.cursor()

    cursor.executemany(sql,all_data)
    db.commit()
    cursor.close()
    db.close()

def get_codes():
    mydb = mysql.connector.connect(
        user=f"{username}",
        password=f"{password}",
        database="vietstock2"
    )

    mycursor = mydb.cursor()

    mycursor.execute("SELECT CompanyID FROM company")

    myresult = mycursor.fetchall()

    code_list = []
    for result in myresult:
        code_list.append(result[0])
    return code_list

def check_data(all_data,code):
    mydb = mysql.connector.connect(
        user=f"{username}",
        password=f"{password}",
        database="vietstock2"
    )
    mycursor = mydb.cursor()
    mycursor.execute(f"SELECT * FROM vietstock2.transaction where CompanyID='{code}'")
    results = mycursor.fetchall()
    mycursor.close()
    mydb.close()
    new_data = []
    update_data = []

    old = 0
    update = 0
    for datas in all_data:
        check = False

        for result in results:
            # match date and content

            if datas[2] == result[3] and  (str(datas[3]) == str(result[4]) or str(datas[3]) == str(result[4]).split('.')[0]):
                check = True
                old += 1
                break
            # match only date
            elif datas[2] == result[3]:
                check = True
                update += 1
                listA = [result[0]]
                update_data.append(listA + datas)
                break
        if check:
            continue
        new_data.append(datas)

    return new_data, update_data, old, update

def crawl_data(code):
    print(f"Begin Crawling {code}")
    url = "https://finance.vietstock.vn/data/gettradingresult"
    payload = f"Code={code}&OrderBy=&OrderDirection=desc&PageIndex=1&PageSize=1000&FromDate=1990-05-17&ToDate={year}-{month}-{day}&ExportType=default&Cols=MC%2CTGG%2CDC%2CTGPTG%2CCN%2CTN%2CGDC%2CKLGDKL%2CGTGDKL%2CKLGDTT%2CGTGDTT&ExchangeID=1&__RequestVerificationToken=AQZ-m5eyEr2gSEVeP2a3NaNCSI8sOca3gWGfMiIlXlRB8hZJWvitvFV-yD3OiyB7GFclxSKB50eF-hcqXsU2le6jvfLz0WNEYSohhctzpG2VxutP6_IirSmXMTX6eCjfIFeCUbL7wNOfE8zMRKTbOw2"
    headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Cookie': 'Theme=Light; AnonymousNotification=; _ga=GA1.2.716612831.1651411681; dable_uid=19388140.1602055334485; vst_isShowTourGuid=true; dable_uid=19388140.1602055334485; __gads=ID=e6852974f340fb2d-22c000f119d30085:T=1651411680:RT=1652015976:S=ALNI_MbVKPB002Cjw9VXdiZuzX8l1keL7Q; language=vi-VN; _pbjs_userid_consent_data=3524755945110770; isShowLogin=true; _gid=GA1.2.485492172.1653274137; ASP.NET_SessionId=n1okbmu3buwbypsp3dzwngfo; finance_viewedstock=AAA,; __RequestVerificationToken=W-v9gLjTfYXpvLlmGQlalBMwbkN75JeHbOlFLMupAd-tT9owG2sJbxRX2Z9zrUaBpuRJXoG5l2bpBil1XYVPXqtW1xRPyP4VE0Hd-CtfzGM1; __gpi=UID=0000051c2f1fed3c:T=1651411679:RT=1653359542:S=ALNI_MZog5IYuaybCUb-E_6yQfE1ZXa2_w; vts_usr_lg=9B43387BFB1714DF95A2E101A89F89E7A9A53422F2BC2A867DA7B423E8F2959D728C226CE23E1CCB96B0868A5D6CF4CB203FD16008E52F1FB7B455A605BFB757E90A4D3AEE1880BE4DCED42C56F97552BE857B0D1B748172A3E970A2563B5FA526E5F43EE0B392D6DDF8E3E21DE361D81485C64B31E6D0227E6BD033A9E575BE89AC9EEEE4D9CDD93845320DA4BE7ADC; vst_usr_lg_token=Ri0kwZFn1UitTutAcpfOcw==',
        'Origin': 'https://finance.vietstock.vn',
        'Referer': 'https://finance.vietstock.vn/AAA/thong-ke-giao-dich.htm',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    data = response.json()
    all_data = []

    for d in data['Data']:
        data_store = []
        data_store.append("Vietstock")
        data_store.append(code)
        date = int(d['TradingDate'][6:19])
        date = datetime.datetime.fromtimestamp(date / 1000.0)
        date = str(date)[:10]
        data_store.append(date)
        data_store.append(d['OpenPrice'])
        data_store.append(d['ClosePrice'])
        data_store.append(d['HighestPrice'])
        data_store.append(d['LowestPrice'])
        data_store.append(d['AdjustPrice'])
        data_store.append(d['Change'])
        data_store.append(d['PerChange'])
        data_store.append(d['MT_TotalVol'])
        data_store.append(d['MT_TotalVal'])
        data_store.append(d['PT_TotalVol'])
        data_store.append(d['PT_TotalVal'])

        all_data.append(data_store)



    new_data, update_data, old, update = check_data(all_data, code)
    insert_sql(sql, new_data)
    total = len(all_data)
    new = total - old - update
    print(f"Finish Crawling {code}")
    return total, old, update, new

class Worker_Transaction(QObject):

    update_process = QtCore.pyqtSignal(dict)
    quick_update = QtCore.pyqtSignal(dict)
    quick_update2 = QtCore.pyqtSignal(dict)
    finish_smallList = QtCore.pyqtSignal(dict)

    def __init__(self, number, user, password):
        super().__init__()
        self.number = number
        self.user = user
        self.password = password


    def runTransaction(self):
        setUsernameAndPassword(self.user,self.password)
        code_list = get_codes()
        lists = [code_list[i:i + 100] for i in range(0, len(code_list), 100)]
        count_number = 1
        count_page = 0

        for small_list in lists:
            all_old_data = 0
            all_update_data = 0
            all_new_data = 0
            all_data = 0

            self.update_process.emit({'number':self.number,'index':count_number-1,'list1':f"List {count_number} from {count_page} to {count_page+len(small_list)} ",
                                      'list3':f"Begin Crawling from from {count_page} to {count_page+len(small_list)}\n"})

            with ThreadPoolExecutor(max_workers=2) as executor:
                for code, (number_page,old_data,update_data,new) in zip(small_list, executor.map(crawl_data, small_list)):
                    self.quick_update.emit({'number':self.number,'data':f"    Finish Crawling {code}\n"})
                    self.quick_update2.emit({'number':self.number,'row':count_number-1,'index':small_list.index(code),'old':old_data,'all':number_page,'update':update_data,'new':new})
                    all_data+=number_page
                    all_old_data+=old_data
                    all_update_data +=update_data
                    all_new_data += new
            self.finish_smallList.emit({'number':self.number,'index':count_number-1,'all':all_data,'new':all_new_data,'update':all_update_data,'old':all_old_data})
            count_page+=len(small_list)
            count_number+=1