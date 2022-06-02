import requests
import time
import csv
import sys
import os
import mysql.connector
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import QObject,QThread,pyqtSignal
from PyQt5.QtWidgets import *
import CompanyVietStock
from concurrent.futures import ThreadPoolExecutor

sql_financial = "INSERT INTO financialindicator (CompanyID,Source,Time,EPS,BVPS,MarketPriceToEarningsIndex," \
                    "MarketPriceIndexOnBookValue,MarginGrossProfitMargin,ProfitMarginOnNetRevenue,ROEA,ROAA," \
                    "CurrentShortTermPayoutRatio,InterestSolvency,RatioOfDebtToTotalAssets,RatioOfDebtToEquity ) " \
                    "VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

username = "root"
password = "2"

def setUsernameAndPassword(user,passw):
    global username
    username = user
    global password
    password = passw

def insert_sql(sql, all_data):
    db = mysql.connector.connect(
            host="localhost",
            user=f"{username}",
            password=f"{password}",
            database="vietstock2"
        )

    cursor = db.cursor()
    cursor.execute('SET FOREIGN_KEY_CHECKS=0')
    cursor.executemany(sql,all_data)
    cursor.execute('SET FOREIGN_KEY_CHECKS=1')
    db.commit()
    cursor.close()
    db.close()



def checkNoneAndDivideBY4(value):
    if value is None:
        return None
    return value / 4

def check_data(all_data,code):
    mydb = mysql.connector.connect(
        user=f"{username}",
        password=f"{password}",
        database="vietstock2"
    )
    mycursor = mydb.cursor()
    mycursor.execute(f"SELECT * FROM vietstock2.financialindicator where CompanyID='{code}'")
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

def get_year_datas(code):
    count = 1
    year_store = []

    all_data_2 = []

    print(f'Start scrape data {code} year')
    while True:
        url = "https://finance.vietstock.vn/data/financeinfo"
        payload = f"Code={code}&ReportType=BCTT&ReportTermType=1&Unit=1000&Page={count}&PageSize=4&__RequestVerificationToken=xPmnK73rimH-R-lWB0mz5e4ZjafyTddqDYLeMCuZuOmVLe884pvYtBrU11ko4-Jq-3SzSBPJdUDEEUg1eMVaVvd3kmJuzR7tY9RYKRxO1bU1"
        headers = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Cookie': 'language=vi-VN; Theme=Light; AnonymousNotification=; _ga=GA1.2.716612831.1651411681; __gads=ID=e6852974f340fb2d-2294dd7381d20016:T=1651411680:RT=1651411680:S=ALNI_MYJmACKn5UjBrrfsfAch99JRUcseg; dable_uid=19388140.1602055334485; _gid=GA1.2.1954122592.1651680972; __gpi=UID=0000051c2f1fed3c:T=1651411679:RT=1651681020:S=ALNI_MZog5IYuaybCUb-E_6yQfE1ZXa2_w; vst_usr_lg_token=fIPqFRXSu0iE6ySa7KJ+Qg==; vst_isShowTourGuid=true; ASP.NET_SessionId=alhzw1iazay5zykzxgs22uap; __RequestVerificationToken=_v4pE0mQYVQqsLVd1fvI1Ssfu8jkqUugE2fdKolWX6BOrVVazKsJWovl5HskW_kMV0dNSVjx0ES9kXn_ITwCBhJKWKvxoAD_reFF9eNA9_01; finance_viewedstock=A32,AAA,',
            'Origin': 'https://finance.vietstock.vn',
            'Referer': 'https://finance.vietstock.vn/A32/tai-chinh.htm',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest',
            'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="100", "Google Chrome";v="100"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        data = response.json()
        # break empty content
        if not data[0]:
            break
        # break wrong data
        if len(data[1]) > 3:
            break
        length = len(data[0]) - 1
        ### get year

        data_store_2 = [[], [], [], []]

        check_year_store = [[], [], [], []]
        for i in range(4):

            data_store_2[i].append(code)
            data_store_2[i].append('Vietstock2')
            if length - i < 0:
                letter = None
            else:
                letter = data[0][length - i]['YearPeriod']

            data_store_2[i].append(letter)


            if letter in year_store:
                check_year_store[i].append(False)
            else:
                year_store.append(letter)
                check_year_store[i].append(True)


        for header in data[1]['Chỉ số tài chính']:
            data_store_2[0].append(checkNoneAndDivideBY4(header['Value1']))
            data_store_2[1].append(checkNoneAndDivideBY4(header['Value2']))
            data_store_2[2].append(checkNoneAndDivideBY4(header['Value3']))
            data_store_2[3].append(checkNoneAndDivideBY4(header['Value4']))


        for i in range(4):
            if check_year_store[i][0] is True:
                all_data_2.append(data_store_2[i])
        count += 1

    all_data_2V2 = []

    ### year to quarter

    for i in range(len(all_data_2)):
        for j in range(1, 5):
            all_data_2[i][2] = f"Q{5 - j}{str(all_data_2[i][2])[-4:]}"
            all_data_2V2.append(all_data_2[i][:])
    total, old, update, new = 0,0,0,0
    if not all_data_2V2 :
        return total, old, update, new
    if  len(all_data_2V2[0]) == 15:
        new_data, update_data, old, update = check_data(all_data_2V2, code)

        insert_sql(sql_financial, new_data)

        total = len(all_data_2V2)
        new = total - old - update
    print(f' Done scrape data {code} year')
    return total, old, update, new



def get_quarter_datas(code):
    count = 1
    quarteryear_store = []
    all_data_2 = []
    total, old, update, new = 0, 0, 0, 0
    print(f'Start scrape data {code} quarter')
    while True:
        url = "https://finance.vietstock.vn/data/financeinfo"

        payload = f"Code={code}&ReportType=BCTT&ReportTermType=2&Unit=1000&Page={count}&PageSize=1&__RequestVerificationToken=vakFPMjxW5zETrZyyv0NNA8RoiG_IUlb0lX8cmeLCiQuOVI5JXZ9_y7P-SBPBw8yuhyAVqhEapGIp7NxV_y_Cb_y6SG663CykYoEjTXt3z81"
        headers = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Cookie': 'language=vi-VN; Theme=Light; AnonymousNotification=; _ga=GA1.2.716612831.1651411681; __gads=ID=e6852974f340fb2d-2294dd7381d20016:T=1651411680:RT=1651411680:S=ALNI_MYJmACKn5UjBrrfsfAch99JRUcseg; dable_uid=19388140.1602055334485; _gid=GA1.2.1954122592.1651680972; vst_isShowTourGuid=true; isShowLogin=true; __gpi=UID=0000051c2f1fed3c:T=1651411679:RT=1651815160:S=ALNI_MZog5IYuaybCUb-E_6yQfE1ZXa2_w; vst_usr_lg_token=U0FSFssSlkCiJojPwBLOpw==; ASP.NET_SessionId=puyqzwwizbtpxnxbjytvaiu0; finance_viewedstock=AAA,; __RequestVerificationToken=2hhzSiXBvN6HztPV5y2p8b0TvUQog8V5IlQ6oor9ddPhMnbAY-Fo_vOikjufv3B3fxKYwFxL_Z8wus06R8ESi2b3PhIfvE--pf5DSAZUGOc1',
            'Origin': 'https://finance.vietstock.vn',
            'Referer': 'https://finance.vietstock.vn/AAA/tai-chinh.htm',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest',
            'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        data = response.json()
        ### no data , break
        if not data[0]:
            break
        ### wrong import data, break
        if len(data[1]) > 3:
            break
        length = len(data[0]) - 1

        ### get year

        data_store_2 = [[], [], [], []]

        check_quateryear_store = [[], [], [], []]
        for i in range(4):
            data_store_2[i].append(code)
            data_store_2[i].append('Vietstock2')
            if length - i < 0:
                letter = None
            else:
                letter = f"{data[0][length - i]['TermCode']}{data[0][length - i]['YearPeriod']}"
            data_store_2[i].append(letter)
            if letter in quarteryear_store:
                check_quateryear_store[i].append(False)
            else:
                quarteryear_store.append(letter)
                check_quateryear_store[i].append(True)

        ### get data


        for header in data[1]['Chỉ số tài chính']:
            data_store_2[0].append(header['Value1'])
            data_store_2[1].append(header['Value2'])
            data_store_2[2].append(header['Value3'])
            data_store_2[3].append(header['Value4'])

        for i in range(4):
            if check_quateryear_store[i][0] is True:

                all_data_2.append(data_store_2[i])

        count += 1

    if not quarteryear_store:
        print(f'quarter {code} is empty, start crawling year')
        total, old, update, new = get_year_datas(code)
    else:

        if len(all_data_2[0]) == 15:
            new_data, update_data, old, update = check_data(all_data_2,code)

            insert_sql(sql_financial, new_data)

            total = len(all_data_2)
            new =  total - old - update
            # print(f'{code} {total} {old} {update} {new}')
        print(f' Done scrape data {code} quarter')

    return total, old, update, new


class Worker_Financial(QObject):

    update_process = QtCore.pyqtSignal(dict)
    quick_update = QtCore.pyqtSignal(dict)
    quick_update2 = QtCore.pyqtSignal(dict)
    finish_smallList = QtCore.pyqtSignal(dict)

    def __init__(self, number, user, password):
        super().__init__()
        self.number = number
        self.user = user
        self.password = password


    def runFinancial(self):
        setUsernameAndPassword(self.user,self.password)
        code_list = CompanyVietStock.get_codes()
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
                for code, (number_page,old_data,update_data,new) in zip(small_list, executor.map(get_quarter_datas, small_list)):
                    self.quick_update.emit({'number':self.number,'data':f"    Finish Crawling {code}\n"})
                    self.quick_update2.emit({'number':self.number,'row':count_number-1,'index':small_list.index(code),'old':old_data,'all':number_page,'update':update_data,'new':new})
                    all_data+=number_page
                    all_old_data+=old_data
                    all_update_data +=update_data
                    all_new_data += new
            self.finish_smallList.emit({'number':self.number,'index':count_number-1,'all':all_data,'new':all_new_data,'update':all_update_data,'old':all_old_data})
            count_page+=len(small_list)
            count_number+=1






