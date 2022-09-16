import pandas as pd

import etl_def
import mysql_def2
import crawler

class KwEtl:
    def __init__(self, user):
        self.user=user   
    
    # 키워드 API ETL 함수 type 0은 단일데이터, 1은 연관데이터
    def api_etl(self, repkws, table_name, amt):
        df =  etl_def.NaKwET(etl_def.AccInfo('kunokw').acc_info).kw_vol(repkws, amt)        
        mysql_def2.PytoMysql(mysql_def2.Accinfo(self.user).accinfo).insert_into(table_name, df.values.tolist())
        print("Data ETL 완료")        

    # 대량키워드 kwde crawl 함수
    def crl_etl(self, repkws, table_name, attype, amt):
        df = crawler.KwDeCrawler(amt).deinfo_df(repkws, attype)
        mysql_def2.PytoMysql(mysql_def2.Accinfo(self.user).accinfo).insert_into(table_name, df.values.tolist())
        print("Data ETL 완료") 
