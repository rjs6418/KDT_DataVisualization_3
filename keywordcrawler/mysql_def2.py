import MySQLdb
from re import L
from tqdm import tqdm

#connect info 
class Accinfo:   
    
    def __init__(self, account):    
        self.accinfo=self.__acc_list[account]
            
    __acc_list = {
                'kuno':{'host':"ec2", 'user':"", 'passwd':"", 'db':"crawl_data"},
                'kuno_new':{}
                    }   

class PytoMysql:
	
	def __init__(self, infod):
		self.host = infod['host']
		self.user = infod['user']
		self.passwd = infod['passwd']
		self.db = infod['db']
		
	def __get_conn(self):
		conn = MySQLdb.connect(
			host = self.host,
			user = self.user,
			passwd = self.passwd,
			db = self.db
		)
		return conn

		#테이블 생성 명령어 함수
	def __cct(self, table_name, table_attb):
		tbinfo = DfClng(table_attb)
		table_att=tbinfo.col

		att_addtype=[]  
		for i in range(len(table_att)):
			att_addtype += [table_att[i] +' '+ tbinfo.tycol[i]]
	  
		z=0
		comm_create = "CREATE TABLE " + table_name + " ("
		while z != len(table_att):
			comm_create += att_addtype[z]
			if z < len(table_att)-1:
				comm_create += ", "
			else :
				comm_create += ")"  
			z += 1    
		return comm_create  

	#테이블 생성 함수
	def create_table(self, table_name, table_attb):
		conn = self.__get_conn()
		cursor = conn.cursor()
		comm_create = self.__cct(table_name, table_attb)    
		cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
		print(comm_create)
		cursor.execute(comm_create)
		cursor.close()
		conn.close()
		print(f'[{table_name}]table이 정상적으로 (재)생성되었습니다.')
		 
	#데이터 삽입 함수
	def insert_into(self, table_name, insert_data):
		conn = self.__get_conn()
		cursor = conn.cursor()
		if insert_data == []:
			print('삽입할 데이터가 없습니다.')
			quit()
		l_d=len(insert_data[0])    
		for degree in tqdm(insert_data):
			z=0
			comm_insert = "INSERT INTO " + table_name + " VALUES("    
			while z!=l_d:
				comm_insert += '\"'
				comm_insert += str(degree[z])
				comm_insert += '\"'    
				if z < l_d-1:
					comm_insert += ", "
				else :
					comm_insert += ")"
				z += 1
			cursor.execute(comm_insert)
		conn.commit()
		cursor.close()
		conn.close()
		print(f'[{table_name}] table에 수집된 data가 정상적으로 [삽입]되었습니다.') 

# 데이터 정제 클래스
class DfClng:
    def __init__(self, table_attb):
        self.col = self.cold[table_attb]
        self.rmcol = self.rmcold[table_attb]
        self.rncol = self.rncold[table_attb]
        self.swcol = self.swcold[table_attb]
        self.tycol = self.tycold[table_attb]
    
    #데이터 정제함수
    def df_clng(self, df): 
        df.drop(self.rmcol, axis=1, inplace=True)
        df.rename(self.rncol, axis=1, inplace=True)
        df = df[self.swcol]
        return df   
    
    #데이터 정제 standard
    #import
    cold = {'kw_vol':['relKeyword', 'monthlyPcQcCnt', 'monthlyMobileQcCnt', 'monthlyAvePcClkCnt', 'monthlyAveMobileClkCnt', 'monthlyAvePcCtr', 'monthlyAveMobileCtr', 'plAvgDepth', 'compIdx', 'date'],
           'kw_mth':['monthlyProgressPcQcCnt', 'monthlyProgressMobileQcCnt', 'monthlyLabel', 'relKeyword'][::-1]+['date'],
           'kw_user':['monthlyPcQcCnt', 'monthlyMobileQcCnt', 'genderType', 'ageGroup', 'relKeyword'][::-1]+['date']
           } 
    
    tycold = {'kw_vol':['text']+['int']+['int']+['decimal(7, 1)']+['decimal(7, 1)']+['decimal(7, 1)']+['decimal(7, 1)']+['int']+['text']+['date'],
             'kw_mth':['text']+['text']+['int']+['int']+['date'], 
             'kw_user':['text']+['text']+['text']+['int']+['int']+['date'] 
             }    
    #export
    rmcold = {'kw_vol':['monthlyAvePcCtr', 'monthlyAveMobileCtr', 'plAvgDepth', 'compIdx'], 
             'kw_mth':[], 
             'kw_user':[]
             }
    
    rncold = {'kw_vol':{'relKeyword': 'repkw', 'monthlyAvePcClkCnt': 'adclick_pc', 'monthlyAveMobileClkCnt': 'adclick_mo', 'monthlyPcQcCnt': 'search_pc', 'monthlyMobileQcCnt': 'search_mo'}, 
             'kw_mth':{'relKeyword': 'repkw', 'monthlyLabel':'mth', 'monthlyProgressMobileQcCnt':'search_mo', 'monthlyProgressPcQcCnt':'search_pc'}, 
             'kw_user':{'relKeyword':'repkw', 'ageGroup':'age', 'genderType':'gender', 'monthlyPcQcCnt':'search_pc', 'monthlyMobileQcCnt':'search_mo'},
             }

    swcold = {'kw_vol':['repkw', 'search_pc', 'search_mo', 'adclick_pc', 'adclick_mo'], 
             'kw_mth':['repkw', 'mth', 'search_pc', 'search_mo'], 
             'kw_user':['repkw', 'gender', 'age', 'search_pc', 'search_mo'], 
             }
    