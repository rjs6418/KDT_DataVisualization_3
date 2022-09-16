from logging import error
import time
import json
import pandas as pd
from tqdm import tqdm
from datetime import date

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver import DesiredCapabilities
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

class KwDeCrawler:
    
    def __init__(self, amt):
        if amt%1==0 and 1<=amt<=100:
            self.amt = amt
        else:    
            print('error: Need Number(1~100)')   
            quit()
        self.__ID = '####'
        self.__PW = '####'
        self.__data_tt = []     #수집 데이터
        self.nid_kws = []     #수집 불가 키워드
        self.nod_kws = []     #정보 없는 키워드
        self.nud_kws = []     #non user data 키워드
        self.nmd_kws = []     #non mth data 키워드
        self.tislp = 1        #수집 속도 제어
    
    #log_filter fuction
    def __log_filter(self, log_):
        return (
            # is an actual response
            log_["method"] == "Network.responseReceived"
            # and json
            and "json" in log_["params"]["response"]["mimeType"]
        )

    #selenium naverkw deinfo datatype:list   
    def kw_deinfo(self, repkws):    
        
        # produce driver 
        capabilities = DesiredCapabilities.CHROME
        capabilities["goog:loggingPrefs"] = {"performance": "ALL"}
        
        options = webdriver.ChromeOptions()
        options.add_argument("--no-sandbox")
        # options.add_argument("--headless")
        options.add_argument('--blink-settings=imagesEnabled=false')
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        options.add_experimental_option(
            "prefs",
            {
                "download.prompt_for_download": False,
                'profile.default_content_setting_values.automatic_downloads': False,
                "download.directory_upgrade": True,
                "safebrowsing_for_trusted_sources_enabled": False,
                "safebrowsing.enabled": False,
            },
        )
        
        # driver = webdriver.Chrome(desired_capabilities=capabilities, options=options, executable_path="./chromedriver")
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), desired_capabilities=capabilities, options=options)

        # connect naverad
        driver.get("https://searchad.naver.com/")
        id = driver.find_element(By.ID,"uid")
        id.send_keys(self.__ID)
        pw = driver.find_element(By.ID,"upw")
        pw.send_keys(self.__PW)
        pw.send_keys(Keys.ENTER)

        # connect keywordtool
        driver.get("https://manage.searchad.naver.com/customers/1973459/tool/keyword-planner")
        time.sleep(0.1*self.tislp)
        
        # search kwdata
        L = len(repkws)
        repkws += ['']
        try:
            z = 0
            for repkw in tqdm(repkws):
                keyword_xpath = "/html/body/div[1]/div/div[2]/div/div/div[2]/div[1]/div[1]/div[2]/div/div[1]/div/textarea"
                driver.find_element(By.XPATH, keyword_xpath).clear()
                driver.find_element(By.XPATH, keyword_xpath).send_keys(repkw)
                time.sleep(0.05*self.tislp)
                button_xpath = "/html/body/div[1]/div/div[2]/div/div/div[2]/div[1]/div[1]/div[3]/button"
                driver.find_element(By.XPATH, button_xpath).click()
                z += 1      
                if z == L+1:break
                if z == 100:raise          
                time.sleep(0.35*self.tislp)

                #collect kw_deinfo log
                i=0                   
                while i != self.amt:
                    deinfs_xpath = "/html/body/div[1]/div/div[2]/div/div/div[2]/div[1]/div[2]/div[2]/div/div[2]/table/tbody/tr[{}]/td[2]/div/span".format(i+1)                  
                    try:
                        deinfp=driver.find_element(By.XPATH, deinfs_xpath)
                        deinfp.click()
                    except:
                        try:
                            if i == 0:
                                time.sleep(0.5*self.tislp)
                                deinfp=driver.find_element(By.XPATH, deinfs_xpath)
                                deinfp.click()
                            else:break    
                        except:   
                            print(f"collecting-\033[95mFAILURE\033[0m-({i}/{self.amt}-{z}/{L})-[{repkw}]-(\033[95merror: 수집불가\033[0m)") 
                            self.nid_kws += [repkw]
                            time.sleep(3.15*self.tislp)
                            break
                            
                    time.sleep(0.25*self.tislp)   
                    logs_raw = driver.get_log("performance")
                    logs = [json.loads(lr["message"])["message"] for lr in logs_raw]
                    for log in filter(self.__log_filter, logs):
                        request_id = log["params"]["requestId"]
                        resp_url = log["params"]["response"]["url"]
                    if "keywordstool" in resp_url:
                        kwinfo = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": request_id})
                        data_log=json.loads(kwinfo['body'])
                        relkw = data_log["keywordList"][0]["relKeyword"]          
                        i += 1
                        
                    if data_log["keywordList"][0]['monthlyProgressList']['monthlyLabel'] == []:
                        self.nmd_kws += [relkw]
                    if data_log["keywordList"][0]['userStat']['ageGroup'] + data_log["keywordList"][0]['userStat']['genderType'] == []:
                        self.nud_kws += [relkw]                        
                    if data_log["keywordList"][0]['userStat']['ageGroup'] + data_log["keywordList"][0]['userStat']['genderType'] + data_log["keywordList"][0]['monthlyProgressList']['monthlyLabel'] == []:
                        print(f"collecting-\033[95mFAILURE\033[0m-({i}/{self.amt}-{z}/{L})-[{relkw}]-(\033[95merror: 정보없음\033[0m)")  
                        self.nod_kws += [relkw]
                        time.sleep(3.4*self.tislp)
                        continue   
                    
                    try:
                        close_button_xpath = ("/html/body/div[4]/div/div[1]/div/div/div[1]/div[2]/button")
                        driver.find_element(By.XPATH, close_button_xpath).click()
                    except:
                        time.sleep(0.4*self.tislp)
                        close_button_xpath = ("/html/body/div[4]/div/div[1]/div/div/div[1]/div[2]/button")
                        driver.find_element(By.XPATH, close_button_xpath).click()
                        
                    self.__data_tt.append(data_log)
             
        except:
            z += -1
            if z == -1:
                z += 1
                print('error: {}'.format('\033[91m접속실패\033[0m'))
                print('\033[34m재접속\033[0m')
            elif z == 0:    
                print('error: {}'.format('\033[91m수집실패\033[0m'))
                print('\033[34m재수집\033[0m')
            elif z == 99:  
                self.tislp += -0.0125 
                print('\033[34m메모리정리\033[0m')
            else:    
                print(f"\033[91merror: 페이지오류\033[0m-collecting-\033[91mFAILURE\033[0m-({i}/{self.amt}-{z+1}/{L})-[\033[91m{repkws[z]}\033[0m]")
                print('[\033[96m{}\033[0m] keyword까지 정상수집'.format(repkws[z-1]))
                print('\033[94m이어서 수집하기\033[0m')
                
            self.tislp += 0.0125
            try:driver.close()
            except:pass
            self.kw_deinfo(repkws[z:-1])
                
        return self.__data_tt

    # selenium data packing$trans 
    def deinfo_df(self, repkws, attype):
        dt_tt = []       
        while repkws:       
            slum_dt=self.kw_deinfo(repkws)
            if attype == 'kw_mth':
                atinfo, stdinfo = 'monthlyProgressList', 'monthlyLabel'
                self.ndd_kws=self.nid_kws + self.nod_kws + self.nmd_kws
            elif attype == 'kw_user':
                atinfo, stdinfo = 'userStat', 'genderType'
                self.ndd_kws=self.nid_kws + self.nod_kws + self.nud_kws
            else:atinfo = None 
            
            dt_index=[]
            for data in slum_dt:
                data["keywordList"][0][atinfo]['relKeyword']=[(data["keywordList"][0]["relKeyword"])]*len(data["keywordList"][0][atinfo][stdinfo])
                dt_index += pd.DataFrame(data["keywordList"][0][atinfo]).to_dict('records')
            dt_tt += dt_index
            df = pd.DataFrame(dt_index)
            repkws = list(set(repkws) - set(self.ndd_kws) - set(df['relKeyword'].values.tolist()) - {''}) 
            if repkws: self.amt=1; self.__data_tt=[]; print(f'키워드 누락 [\033[91m{len(repkws)}\033[0m]개\n\033[94m누락 키워드 재수집\033[0m')
        df_tt = pd.DataFrame(dt_tt).drop_duplicates(ignore_index = True)
        df_tt = df_tt[list(df_tt.columns.values)[::-1]]
        df_tt['date']=date.today()
        
        return df_tt
    
print(KwDeCrawler(10).deinfo_df(["홍대맛집", "구리맛집"], "kw_user"))
