import etl_def
import csv
import time

# 키워드 창출 반복함수
def add_kw(kw_list_rdy, times, csvcsv):
    NakwET = etl_def.NaKwET(etl_def.AccInfo('kunokw').acc_info)    
    with open(csvcsv, 'w', newline='') as f:    
        writer =csv.writer(f)   
        for rdykw in kw_list_rdy:
            writer.writerow([rdykw])
        t=0
        while t < times:
            t += 1     
            new_kws = list(set(NakwET.kw_vol(kw_list_rdy, None)['relKeyword'].tolist()))
            if t != times:
                kw_list_rdy += new_kws
                continue 
            for c in new_kws:
                try:
                    writer.writerow([c])
                except:
                    print(f"error: can\'t save -- {c}")                           

kw_list_rdy = []

add_kw(kw_list_rdy, 3, 'nkw.csv')
