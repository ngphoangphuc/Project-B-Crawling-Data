import requests
import mysql.connector
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import QObject,QThread,pyqtSignal
import CompanyVietStock

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
sql_industry_table = "INSERT INTO industry ( Source,CompanyID," \
                     "Level_1_Industry, Level_2_Industry, Level_3_Industry)" \
                     " VALUES (%s,  %s, %s, %s, %s)"

username = "1"
password = "2"

def setUsernameAndPassword(user,passw):
    global username
    username = user
    global password
    password = passw

def insert_sql(sql, data):
    db = mysql.connector.connect(
            user=f"{username}",
            password=f"{password}",
            database="vietstock2"
        )
    cursor = db.cursor()
    cursor.execute('SET FOREIGN_KEY_CHECKS=0')
    cursor.execute(sql, data)
    cursor.execute('SET FOREIGN_KEY_CHECKS=1')
    db.commit()
    cursor.close()
    db.close()

def crawlIndustry(code_list):
    industry_all_data = []
    codes = []
    count = 1
    while True:
        payload = f"catID=0&industryID=0&page={count}&pageSize=50&type=1&code=&businessTypeID=0&orderBy=Code&orderDir=ASC&__RequestVerificationToken=03gEoeIcjSo0mAyhIYZ_mh0wf42jYZVuPNs_jOVfVBzoHFZGYbYbB1D-y7adeGx9lJZTnGAilu4y8IJMBqWwZDd3LK-15dlpcDC9qkAaYZkYlUunQAInj_g-Zia6CdL4iNlNdO9dbW7duO2hvDFanA2"
        response = requests.request("POST", url, headers=headers, data=payload)
        data = response.json()
        if not data:
            break

        for i in range(len(data)):
            industry_data = []
            if data[i]['Code'] not in code_list:
                continue
            codes.append(data[i]['Code'] )
            industry_data.append('Vietstock')
            industry_data.append(data[i]['Code'])
            industry_data.append(data[i]['IndustryName1'])
            industry_data.append(data[i]['IndustryName2'])
            industry_data.append(data[i]['IndustryName3'])
            industry_all_data.append(industry_data)
            # print(f'industry crawled {industry_data[0]}')

        count+=1
    return industry_all_data,codes

def check_data(data,code):
    mydb = mysql.connector.connect(
        user=f"{username}",
        password=f"{password}",
        database="vietstock2"
    )
    mycursor = mydb.cursor()
    mycursor.execute(f"SELECT * FROM vietstock2.industry WHERE companyID='{code}';")
    myresult = mycursor.fetchone()
    mycursor.close()
    mydb.close()

    old = 0
    update = 0
    new = 0
    total = 1
    if not myresult:
        new = 1
        # add new sql
        insert_sql(sql_industry_table, data)
    elif data[1] == myresult[2] and data[2] == myresult[3] and data[3] == myresult[4]:
        old =1
    elif data[1] == myresult[2] or data[2] == myresult[3] or data[3] == myresult[4]:
        update =1
        # update sql



    return total,old,update,new

class worker_industry(QObject):
    update_process = QtCore.pyqtSignal(dict)
    quick_update = QtCore.pyqtSignal(dict)
    quick_update2 = QtCore.pyqtSignal(dict)
    finish_smallList = QtCore.pyqtSignal(dict)

    def __init__(self, number, user, password):
        super().__init__()
        self.number = number
        self.user = user
        self.password = password

    def runIndustry(self):
        setUsernameAndPassword(self.user,self.password)
        code_list = CompanyVietStock.get_codes()
        lists = [code_list[i:i + 100] for i in range(0, len(code_list), 100)]
        count_number = 1
        count_page = 0

        data_list,codes = crawlIndustry(code_list)

        for small_list in lists:
            all_old_data = 0
            all_update_data = 0
            all_new_data = 0
            all_data = 0

            self.update_process.emit({'number':self.number,'index':count_number-1,'list1':f"List {count_number} from {count_page} to {count_page+len(small_list)} ",
                                      'list3':f"Begin Crawling from from {count_page} to {count_page+len(small_list)}\n"})
            for code in small_list:
                if code not in codes:
                    continue
                number_page, old_data, update_data, new =check_data(data_list[codes.index(code)],code)
                self.quick_update.emit({'number':self.number,'data':f"    Finish Crawling {code}\n"})
                self.quick_update2.emit({'number':self.number,'row':count_number-1,'index':small_list.index(code),'old':old_data,'all':number_page,'update':update_data,'new':new})
                all_data+=number_page
                all_old_data+=old_data
                all_update_data +=update_data
                all_new_data += new
            self.finish_smallList.emit({'number':self.number,'index':count_number-1,'all':all_data,'new':all_new_data,'update':all_update_data,'old':all_old_data})
            count_page+=len(small_list)
            count_number+=1