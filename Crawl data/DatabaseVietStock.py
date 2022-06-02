import mysql.connector
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import QObject,QThread,pyqtSignal


create_company_table = "CREATE TABLE IF NOT EXISTS company (CompanyID CHAR(20) PRIMARY KEY,Source VARCHAR(10)," \
                       "CompanyName VARCHAR(255)," \
                       "Exchange VARCHAR(10) ,Link MEDIUMTEXT)"

create_industry_table = "CREATE TABLE IF NOT EXISTS industry(IndustryID INT PRIMARY KEY AUTO_INCREMENT," \
                        "Source VARCHAR(10)," \
                        "CompanyID CHAR(20), Level_1_Industry varchar(100)," \
                        "Level_2_Industry varchar(100), Level_3_Industry varchar(100)," \
                        "FOREIGN KEY(CompanyID) REFERENCES company(CompanyID))"

money_value = "DOUBLE default 0"
create_businessResult_table = "CREATE TABLE IF NOT EXISTS businessResult (ID INT PRIMARY KEY AUTO_INCREMENT," \
                              "CompanyID CHAR(20)," \
                              f"Time VARCHAR(10),Net_Sales_And_Service_Provision {money_value}, " \
                              f"Cost_Of_Goods_Sold {money_value}," \
                              f"Gross_Profit_On_Sales_And_Service_Delivery {money_value}," \
                              f"Financial_Operating_Revenue {money_value}, Financial_Expenses {money_value}," \
                              f"Cost_Of_Sales {money_value}, Business_Management_Expenses {money_value}," \
                              f"Net_Profit_From_Business_Activities {money_value}, Other_Profits {money_value}," \
                              f"Profit_Or_Loss_Portion_From_The_Joint_Venture_Affiliate {money_value}," \
                              f"Total_Accounting_Profit_Before_Tax {money_value}," \
                              f"Profit_After_Corporate_Income_Tax {money_value}," \
                              f"After_Tax_Profit_Of_Shareholders_Of_The_Parent_Company {money_value}," \
                              f"Underlying_Earnings_Per_Share {money_value}," \
                              f"FOREIGN KEY(CompanyID) REFERENCES company(CompanyID))"

create_balanceSheetAccounting_table = "CREATE TABLE IF NOT EXISTS balanceSheetAccounting " \
                                      "(ID INT PRIMARY KEY AUTO_INCREMENT,CompanyID CHAR(20),Source VARCHAR(10)," \
                                      "Time VARCHAR(10), " \
                                      f"Short_Term_Assets {money_value}, Money_And_Cash_Equivalents {money_value}," \
                                      f"Short_Term_Financial_Investments {money_value}," \
                                      f"Short_Term_Receivables {money_value}," \
                                      f"Inventory {money_value}, Other_Short_Term_Assets {money_value}," \
                                      f"Long_Term_Assets {money_value}, Fixed_Assets {money_value}," \
                                      f"Investment_Real_Estate {money_value}," \
                                      f"Long_Term_Financial_Investments {money_value}, Total_Assets {money_value}," \
                                      f"Liabilities {money_value}, Short_Term_Debt {money_value}," \
                                      f"Long_Term_Debt {money_value}," \
                                      f"Equity {money_value}, Investment_Capital_Of_Owner {money_value}," \
                                      f"Equity_Surplus {money_value}," \
                                      f"Undistributed_After_Tax_Profit {money_value}," \
                                      f"Benefits_Of_Minority_Shareholders {money_value}," \
                                      f"Total_Capital {money_value}," \
                                      f"FOREIGN KEY(CompanyID) REFERENCES company(CompanyID))"

create_financialIndicator_table = "CREATE TABLE IF NOT EXISTS financialIndicator " \
                                  "(ID INT PRIMARY KEY AUTO_INCREMENT,CompanyID CHAR(20),Source VARCHAR(10)," \
                                  "Time VARCHAR(10), " \
                                  f"EPS {money_value}, BVPS {money_value}," \
                                  f"MarketPriceToEarningsIndex {money_value}, " \
                                  f"MarketPriceIndexOnBookValue {money_value}," \
                                  f"MarginGrossProfitMargin {money_value}, ProfitMarginOnNetRevenue {money_value}," \
                                  f"ROEA {money_value}, ROAA {money_value}, CurrentShortTermPayoutRatio {money_value}," \
                                  f"InterestSolvency {money_value}, RatioOfDebtToTotalAssets {money_value}," \
                                  f"RatioOfDebtToEquity {money_value}, " \
                                  "FOREIGN KEY(CompanyID) REFERENCES company(CompanyID))"

create_articles_table = "CREATE TABLE IF NOT EXISTS articles (ArticlesID INT  PRIMARY KEY AUTO_INCREMENT," \
                        "Source VARCHAR(10), Company CHAR(20), Title MEDIUMTEXT, Time VARCHAR(12), " \
                        "ArticlesLink MEDIUMTEXT," \
                        "Appraisal TINYINT default 0, Valuable TINYINT default 0," \
                        "FOREIGN KEY(Company) REFERENCES company(CompanyID))"

value_exchange1= "DOUBLE default 0"
create_transaction_table = "CREATE TABLE IF NOT EXISTS transaction (ID INT  PRIMARY KEY AUTO_INCREMENT," \
                           "Source VARCHAR(10), CompanyID CHAR(20)," \
                           f"Trading_Date VARCHAR(10), Open_Price {value_exchange1}, Close_Price {value_exchange1}," \
                           f"Highest {value_exchange1}, Lowest {value_exchange1}, Price_Revised {value_exchange1}," \
                           f"Diff_Price {value_exchange1}, Diff_Percent_Price {value_exchange1}," \
                           f"Matching_Volume {value_exchange1},Matching_Value VARCHAR(30)," \
                           f"Put_Through_All_Volume {value_exchange1},Put_Through_All_Value {value_exchange1}," \
                           f"FOREIGN KEY(CompanyID) REFERENCES company(CompanyID))"



class Worker_Database(QObject):

    finish_database = QtCore.pyqtSignal()

    def __init__(self, user,password):
        super().__init__()

        self.user = user
        self.password = password

    def runDatabase(self):
        dbname = "vietstock2"  # vietstock or cafef
        create_db = str("CREATE DATABASE IF NOT EXISTS " + dbname)

        # create database
        db = mysql.connector.connect(user=f"{self.user}", password=f"{self.password}")
        cursor = db.cursor()
        cursor.execute(create_db)

        # create table
        db = mysql.connector.connect(user=f"{self.user}", password=f"{self.password}", database=dbname)
        cursor = db.cursor()
        cursor.execute(create_company_table)
        cursor.execute(create_industry_table)
        cursor.execute(create_businessResult_table)
        cursor.execute(create_balanceSheetAccounting_table)
        cursor.execute(create_financialIndicator_table)
        cursor.execute(create_articles_table)
        cursor.execute(create_transaction_table)
        self.finish_database.emit()
