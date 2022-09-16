import time
import os

import repkw_list
import mysql_def2
import python_mysql


# 키워드리스트
repkws = repkw_list.repkws

Create Table:
PytoMysql = mysql_def2.PytoMysql(mysql_def2.Accinfo('kuno').accinfo)
PytoMysql.create_table('kwvolvol','kw_vol')
PytoMysql.create_table('kwmthmth','kw_mth')
PytoMysql.create_table('kwuseruser','kw_user')

Execute ETL:
kwetl=python_mysql.KwEtl('kuno')
kwetl.api_etl(repkws, 'kwvolvol', 1)
kwetl.crl_etl(repkws, 'kwmthmth', 'kw_mth', 1)
kwetl.crl_etl(repkws, 'kwuseruser', 'kw_user', 1)

컴퓨터 종료:
time.sleep(600)
os.system('shutdown -s -f')
