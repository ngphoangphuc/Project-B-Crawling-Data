import requests
import time
import csv
import sys
import os
import mysql.connector
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import QObject,QThread,pyqtSignal
from concurrent.futures import ThreadPoolExecutor
url = "https://finance.vietstock.vn/data/corporateaz"
headers = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Cookie': 'language=vi-VN; Theme=Light; AnonymousNotification=; _ga=GA1.2.716612831.1651411681; __gads=ID=e6852974f340fb2d-2294dd7381d20016:T=1651411680:RT=1651411680:S=ALNI_MYJmACKn5UjBrrfsfAch99JRUcseg; dable_uid=19388140.1602055334485; ASP.NET_SessionId=wikvha0a3cwzsofzmi1dkjll; __RequestVerificationToken=ePIooPIc-I_PEtPGL5_Zn8K_CgQ9C7hq891W1bSgBNUFgnnuuLJyd8ms-n6gG_QxQV-PxRbL_CiIMrEBUhE_0sqzkc2bCAkDGO0IrOII-441; _gid=GA1.2.1954122592.1651680972; __gpi=UID=0000051c2f1fed3c:T=1651411679:RT=1651681020:S=ALNI_MZog5IYuaybCUb-E_6yQfE1ZXa2_w; vts_usr_lg=D6D56EA902AEF1A5393789194B34469367787DDD7A964C7F393BE82FDF7190AA6E2A2D6CF9245B6FDEE2C3DE1AFAD5CD0A71BF4CAB2A81CD77521F1E590A0E2F70649C76A85B1AF9F6ACCCA9437940DA19B88540C8C4114C19DC5041814CEB0B4B644D1D70612C6BEAEDB223767A280E35F1AADE2FC85004D9D0D2809F593302DB7233F1C5463BBA04A699B21B31C6FA; vst_usr_lg_token=fIPqFRXSu0iE6ySa7KJ+Qg==; finance_viewedstock=ACB,AAM,AAS,AAV,AAA,A32,ABB,ABT,ACV,FPT,YBC,; vst_isShowTourGuid=true; _gat_gtag_UA_1460625_2=1',
            'Origin': 'https://finance.vietstock.vn',
            'Referer': 'https://finance.vietstock.vn/doanh-nghiep-a-z/danh-sach-niem-yet?page=1',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest',
            'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="100", "Google Chrome";v="100"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }

ql_company_table = "INSERT INTO company (CompanyID,Source, CompanyName,Exchange, Link) " \
                      "VALUES (%s,%s,%s,%s,%s)"
username = "1"
password = "2"

def setUsernameAndPassword(user,passw):
    global username
    username = user
    global password
    password = passw



def check_year(code):
    year_store = []
    all_data_1 = []
    all_data_2 = []
    all_data_3 = []


    url = "https://finance.vietstock.vn/data/financeinfo"
    payload = f"Code={code}&ReportType=BCTT&ReportTermType=1&Unit=1000&Page=1&PageSize=4&__RequestVerificationToken=xPmnK73rimH-R-lWB0mz5e4ZjafyTddqDYLeMCuZuOmVLe884pvYtBrU11ko4-Jq-3SzSBPJdUDEEUg1eMVaVvd3kmJuzR7tY9RYKRxO1bU1"
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
    # break empty content
    if not data[0]:
        return False
    # break wrong data
    # if len(data[1]) > 3:
    #     break
    length = len(data[0]) - 1
    ### get year
    data_store_1 = [[], [], [], []]
    data_store_2 = [[], [], [], []]
    data_store_3 = [[], [], [], []]
    check_year_store = [[], [], [], []]
    for i in range(4):
        data_store_1[i].append(code)
        data_store_2[i].append(code)
        data_store_3[i].append(code)
        if length - i < 0:
            letter = None
        else:
            letter = data[0][length - i]['YearPeriod']
        data_store_1[i].append(letter)
        data_store_2[i].append(letter)
        data_store_3[i].append(letter)

        if letter in year_store:
            check_year_store[i].append(False)
        else:
            year_store.append(letter)
            check_year_store[i].append(True)

    ### get data
    for header in data[1]['Kết quả kinh doanh']:
        data_store_1[0].append((header['Value1']))
        data_store_1[1].append((header['Value2']))
        data_store_1[2].append((header['Value3']))
        data_store_1[3].append((header['Value4']))

    for header in data[1]['Cân đối kế toán']:
        data_store_2[0].append((header['Value1']))
        data_store_2[1].append((header['Value2']))
        data_store_2[2].append((header['Value3']))
        data_store_2[3].append((header['Value4']))

    for header in data[1]['Chỉ số tài chính']:
        data_store_3[0].append((header['Value1']))
        data_store_3[1].append((header['Value2']))
        data_store_3[2].append((header['Value3']))
        data_store_3[3].append((header['Value4']))
    for i in range(4):
        if check_year_store[i][0] is True:
            all_data_1.append(data_store_1[i])
            all_data_2.append(data_store_2[i])
            all_data_3.append(data_store_3[i])

    all_data_1V2 = []
    all_data_2V2 = []
    all_data_3V2 = []
    ### year to quarter
    for i in range(len(all_data_1)):
        for j in range(1, 5):
            all_data_1[i][1] = f"Q{5 - j}{str(all_data_1[i][1])[-4:]}"
            ### [:] is to stop copying reference
            all_data_1V2.append(all_data_1[i][:])
    for i in range(len(all_data_2)):
        for j in range(1, 5):
            all_data_2[i][1] = f"Q{5 - j}{str(all_data_2[i][1])[-4:]}"
            all_data_2V2.append(all_data_2[i][:])
    for i in range(len(all_data_3)):
        for j in range(1, 5):
            all_data_3[i][1] = f"Q{5 - j}{str(all_data_3[i][1])[-4:]}"
            all_data_3V2.append(all_data_3[i][:])
    if len(all_data_1V2[0]) == 16 and len(all_data_2V2[0]) == 22 and len(all_data_3V2[0]) == 14:
        return True
    return False


def check_quater(code):

    quarteryear_store = []
    all_data_1 = []
    all_data_2 = []
    all_data_3 = []

    url = "https://finance.vietstock.vn/data/financeinfo"

    payload = f"Code={code}&ReportType=BCTT&ReportTermType=2&Unit=1000&Page=1&PageSize=1&__RequestVerificationToken=vakFPMjxW5zETrZyyv0NNA8RoiG_IUlb0lX8cmeLCiQuOVI5JXZ9_y7P-SBPBw8yuhyAVqhEapGIp7NxV_y_Cb_y6SG663CykYoEjTXt3z81"
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

    ### wrong import data, break
    if len(data[1]) > 3:
        return False
    length = len(data[0]) - 1

    ### get year
    data_store_1 = [[], [], [], []]
    data_store_2 = [[], [], [], []]
    data_store_3 = [[], [], [], []]
    check_quateryear_store = [[], [], [], []]
    for i in range(4):

        data_store_1[i].append(code)
        data_store_2[i].append(code)
        data_store_3[i].append(code)
        if length - i < 0:
            letter = None
        else:
            letter = f"{data[0][length - i]['TermCode']}{data[0][length - i]['YearPeriod']}"
        data_store_1[i].append(letter)
        data_store_2[i].append(letter)
        data_store_3[i].append(letter)
        if letter in quarteryear_store:
            check_quateryear_store[i].append(False)
        else:
            quarteryear_store.append(letter)
            check_quateryear_store[i].append(True)

    ### get data
    for header in data[1]['Kết quả kinh doanh']:
        data_store_1[0].append(header['Value1'])
        data_store_1[1].append(header['Value2'])
        data_store_1[2].append(header['Value3'])
        data_store_1[3].append(header['Value4'])

    for header in data[1]['Cân đối kế toán']:
        data_store_2[0].append(header['Value1'])
        data_store_2[1].append(header['Value2'])
        data_store_2[2].append(header['Value3'])
        data_store_2[3].append(header['Value4'])

    for header in data[1]['Chỉ số tài chính']:
        data_store_3[0].append(header['Value1'])
        data_store_3[1].append(header['Value2'])
        data_store_3[2].append(header['Value3'])
        data_store_3[3].append(header['Value4'])
    for i in range(4):
        if check_quateryear_store[i][0] is True:
            all_data_1.append(data_store_1[i])
            all_data_2.append(data_store_2[i])
            all_data_3.append(data_store_3[i])
    if not quarteryear_store:
        return check_year(code)
    else:
        if len(all_data_1[0]) == 16 and len(all_data_2[0]) == 22 and len(all_data_3[0]) == 14:
            return True
    return False

def insert_sql(sql, data):
    db = mysql.connector.connect(
            user=f"{username}",
            password=f"{password}",
            database="vietstock2"
        )
    cursor = db.cursor()
    cursor.execute(sql, data)
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

def crawl_Company():
    company_all_data = []
    list = []
    count = 1
    while True:
        payload = f"catID=0&industryID=0&page={count}&pageSize=50&type=1&code=&businessTypeID=0&orderBy=Code&orderDir=ASC&__RequestVerificationToken=03gEoeIcjSo0mAyhIYZ_mh0wf42jYZVuPNs_jOVfVBzoHFZGYbYbB1D-y7adeGx9lJZTnGAilu4y8IJMBqWwZDd3LK-15dlpcDC9qkAaYZkYlUunQAInj_g-Zia6CdL4iNlNdO9dbW7duO2hvDFanA2"
        response = requests.request("POST", url, headers=headers, data=payload)
        data = response.json()
        if not data:
            break
        for i in range(len(data)):
            company_data = []
            # Ignore the code if it does not follow the standard, this might slow down the program!
            if check_quater(data[i]['Code']):
                list.append(data[i]['Code'])
                company_data.append(data[i]['Code'])
                company_data.append('VietStock')
                company_data.append(data[i]['Name'])
                company_data.append(data[i]['Exchange'])
                company_data.append(data[i]['URL'])
                company_all_data.append(company_data)



        count+=1
    return company_all_data,list


def check_data(data,code_list):

    old = 0
    update = 0
    new = 0
    total = 1

    if data[0] not in code_list:
        new = 1
        # add new sql
        insert_sql(ql_company_table, data)
    else:
        old = 1


    return total,old,update,new,data[0]

class worker_company(QObject):

    report_size = QtCore.pyqtSignal(list,int)
    update_process = QtCore.pyqtSignal(dict)
    quick_update = QtCore.pyqtSignal(dict)
    quick_update2 = QtCore.pyqtSignal(dict)
    finish_smallList = QtCore.pyqtSignal(dict)


    def __init__(self, number, user, password):
        super().__init__()
        self.number = number
        self.user = user
        self.password = password


    def runCompany(self):
        setUsernameAndPassword(self.user,self.password)
        code_list = get_codes()
        data_list,list= crawl_Company()

        list = [list[i:i + 100] for i in range(0, len(list), 100)]
        self.report_size.emit(list,len(data_list))
        data_list = [data_list[i:i + 100] for i in range(0, len(data_list), 100)]
        count_number = 1
        count_page = 0
        for datas in data_list:
            all_old_data = 0
            all_update_data = 0
            all_new_data = 0
            all_data = 0
            count = 0
            self.update_process.emit({'number': self.number, 'index': count_number - 1,
                                      'list1': f"List {count_number} from {count_page} to {count_page + len(datas)} ",
                                      'list3': f"Begin Crawling from from {count_page} to {count_page + len(datas)}\n"})
            for data in datas:
                number_page, old_data, update_data, new, code= check_data(data,code_list)
                self.quick_update.emit({'number':self.number,'data':f"    Finish Crawling {code}\n"})
                self.quick_update2.emit({'number':self.number,'row':count_number-1,'index':count,'old':old_data,'all':number_page,'update':update_data,'new':new})
                all_data += number_page
                all_old_data += old_data
                all_update_data += update_data
                all_new_data += new
                count+=1
            self.finish_smallList.emit(
                {'number': self.number, 'index': count_number - 1, 'all': all_data, 'new': all_new_data,
                 'update': all_update_data, 'old': all_old_data})
            count_page += len(datas)
            count_number += 1