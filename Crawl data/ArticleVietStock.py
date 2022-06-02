import requests
import bs4
import mysql.connector
from datetime import date
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import QObject,QThread,pyqtSignal
from concurrent.futures import ThreadPoolExecutor

headers = {
    'Accept': 'text/html, */*; q=0.01',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Cookie': 'language=vi-VN; Theme=Light; AnonymousNotification=; _ga=GA1.2.716612831.1651411681; dable_uid=19388140.1602055334485; vst_isShowTourGuid=true; isShowLogin=true; vst_usr_lg_token=U0FSFssSlkCiJojPwBLOpw==; dable_uid=19388140.1602055334485; ASP.NET_SessionId=wfdjde2wpeg0ebwzmljgvzgl; finance_viewedstock=AAA,; __RequestVerificationToken=KU3DSm6xD7hAAeh80ZyFZNlvJM_qAKunPBu8XlOKoXyat7zDR5sUPCli1mhyQ7acNAxcT8lVvv8nB70cLvtqYmUaDEE-kxjng6zXgfPAbeY1; _gid=GA1.2.498756278.1652015942; __gpi=UID=0000051c2f1fed3c:T=1651411679:RT=1652015941:S=ALNI_MZog5IYuaybCUb-E_6yQfE1ZXa2_w; __gads=ID=e6852974f340fb2d-22c000f119d30085:T=1651411680:RT=1652015976:S=ALNI_MbVKPB002Cjw9VXdiZuzX8l1keL7Q',
    'Origin': 'https://finance.vietstock.vn',
    'Referer': 'https://finance.vietstock.vn/AAA/ket-qua-ke-hoach-kinh-doanh.htm',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"'
}

today = date.today()
year = today.strftime("%Y")
month = today.strftime("%m")
day = today.strftime("%d")

username = "1"
password = "2"

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
    cursor.execute('SET FOREIGN_KEY_CHECKS=0')
    cursor.executemany(sql,all_data)
    cursor.execute('SET FOREIGN_KEY_CHECKS=1')
    db.commit()
    cursor.close()
    db.close()

def get_total_page(code = 'AAA'):
    max_page = 1
    url = "https://finance.vietstock.vn/View/PagingNewsContent"
    payload = f"view=1&code={code}&type=1&fromDate=01%2F01%2F1990&toDate={day}%2F{month}%2F{year}&channelID=-1&page=1&pageSize=20"
    response = requests.request("POST", url, headers=headers, data=payload)
    soup = bs4.BeautifulSoup(response.content, 'html.parser')

    div = soup.find('div',{"class": "m-b pull-left"})
    if div is None:
        return max_page
    if len(div.text) > 1:
        max_page = div.text.split(' ')[-1]

    return max_page

def get_articles_url(code = 'AAA', min_page = 1, max_page = 2):

    url = "https://finance.vietstock.vn/View/PagingNewsContent"
    total_data = []

    for i in range(min_page,max_page+1):
        payload = f"view=1&code={code}&type=1&fromDate=01%2F01%2F1990&toDate={day}%2F{month}%2F{year}&channelID=-1&page={i}&pageSize=20"
        response = requests.request("POST", url, headers=headers, data=payload)
        soup = bs4.BeautifulSoup(response.content, 'html.parser')


        for tr in soup.find_all('tr'):
            if tr.find_all('td')[1].find('a') is None:
                break
            data = []
            data.append('Vietstock')
            data.append(code)
            data.append(tr.find_all('td')[1].find('a').text)
            data.append(tr.find_all('td')[0].text)
            data.append("https:"+tr.find_all('td')[1].find('a')['href'])
            if len(data[3]) != 10:
                continue
            total_data.append(data)

    return total_data

def get_codes():
    mydb = mysql.connector.connect(
        user=f"{username}",
        password=f"{password}",
        database="vietstock2"
    )

    mycursor = mydb.cursor()

    mycursor.execute("SELECT CompanyID FROM company")

    myresult = mycursor.fetchall()
    mycursor.close()
    mydb.close()
    code_list = []
    for result in myresult:
        code_list.append(result[0])
    return code_list
def check_article_data(all_data,code):

    mydb = mysql.connector.connect(
        user=f"{username}",
        password=f"{password}",
        database="vietstock2"
    )
    mycursor = mydb.cursor()
    mycursor.execute(f"SELECT * FROM vietstock2.articles where Company='{code}'")
    results = mycursor.fetchall()
    mycursor.close()
    mydb.close()
    new_data = []
    update_data = []

    old = 0
    update = 0
    for datas in all_data:
        check = False
        small_update_data = []
        for result in results:
            # match date and content
            if datas[3] == result[4] and str(datas[2]) == result[3]:
                check = True
                old += 1
                break
            # match only date
            elif datas[3] == result[4]:
                check = True
                update+=1
                listA = [result[0]]
                update_data.append(listA+datas)
                break
        if check:
            continue
        new_data.append(datas)

    return new_data,update_data,old,update



def crawl_Article_data(code):

    print(f'Begin Crawling {code}')
    # print(f"begin {code}")
    max_page = int(get_total_page(code))
    all_data = get_articles_url(code=code,max_page=max_page)
    sql = "INSERT INTO articles (Source,Company, Title, Time, ArticlesLink) " \
          "VALUES (%s,%s,%s,%s,%s)"
    new_data, update_data, old, update = check_article_data(all_data,code)
    # Add new data
    insert_sql(sql,new_data)
    # Update data

    new = len(all_data) - old - update
    print(f'code {code} all {len(all_data)} new {new} old {old} update {update}')
    return len(all_data),old,update,new

class Worker_Article(QObject):

    update_process = QtCore.pyqtSignal(dict)
    quick_update = QtCore.pyqtSignal(dict)
    quick_update2 = QtCore.pyqtSignal(dict)
    finish_smallList = QtCore.pyqtSignal(dict)

    def __init__(self, number,user,password):
        super().__init__()
        self.number = number
        self.user = user
        self.password = password

    def runArticle(self):
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
                for code, (number_page,old_data,update_data,new) in zip(small_list, executor.map(crawl_Article_data, small_list)):

                    self.quick_update.emit({'number':self.number,'data':f"    Finish Crawling {code}\n"})
                    self.quick_update2.emit({'number':self.number,'row':count_number-1,'index':small_list.index(code),'old':old_data,'all':number_page,'update':update_data,'new':new})
                    all_data+=number_page
                    all_old_data+=old_data
                    all_update_data +=update_data
                    all_new_data += new
            self.finish_smallList.emit({'number':self.number,'index':count_number-1,'all':all_data,'new':all_new_data,'update':all_update_data,'old':all_old_data})
            count_page+=len(small_list)
            count_number+=1