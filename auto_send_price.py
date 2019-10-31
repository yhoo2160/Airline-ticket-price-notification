import requests
import sqlite3
import pandas as pd
from datetime import datetime

conn = sqlite3.connect('flight.db')

c = conn.cursor()
cursor = c.execute("SELECT * from API")
for row in cursor:
    print(row)
    if row[1] == 'skyscanner':
        skyscanner_key = row[2]
    elif row[1] == 'ifttt':
        ifttt_key = row[2]

country = "TW"
currency = "TWD"
locale = "zh-TW"
rapidapi_host = "skyscanner-skyscanner-flight-search-v1.p.rapidapi.com"
get_header = {'x-rapidapi-host' : rapidapi_host,
              'x-rapidapi-key': skyscanner_key}

cursor = c.execute("SELECT * from TASK")
for row in cursor:
    print(row)
    task_id = row[0]
    originplace = row[1]
    destinationplace = row[2]
    outboundpartialdate = row[3]
    inboundpartialdate = row[4]
    
    get_link = "https://"+rapidapi_host+"/apiservices/browsedates/v1.0/"+country+"/"+currency+"/"+locale+"/"+originplace+"-sky/"+destinationplace+"-sky/"+outboundpartialdate+"/"+inboundpartialdate
    resp = requests.get(url=get_link, headers=get_header)
    res = resp.json()
    
    list_res = list()
    i = 0
    while i < len(res['Quotes']):
        price = res['Quotes'][i]['MinPrice']
        go_date = datetime.strptime(res['Quotes'][i]['OutboundLeg']['DepartureDate'], "%Y-%m-%dT%H:%M:%S").strftime('%Y%m%d')
        leave_date = datetime.strptime(res['Quotes'][i]['InboundLeg']['DepartureDate'], "%Y-%m-%dT%H:%M:%S").strftime('%Y%m%d')
        list_res.append([price,go_date,leave_date])
        i = i + 1
    
    df = pd.DataFrame(list_res)
    price_min = str(df[df[0] == df[0].min()][0].values[0])
    go_date_min = df[df[0] == df[0].min()][1].values[0]
    leave_date_min = df[df[0] == df[0].min()][2].values[0]
    
    c_res = conn.cursor()
    cursor_res = c_res.execute("SELECT * from RES where ID="+str(task_id))
    flag = 0
    for row in cursor_res:
        flag = 1
        if int(row[1]) > int(float(price_min)):
            link_min = "https://www.skyscanner.com.tw/transport/flights/"+originplace+"/"+destinationplace+"/"+go_date_min+"/"+leave_date_min+"/?adults=1&children=0&adultsv2=1&childrenv2=&infants=0&cabinclass=economy&rtn=1&preferdirects=false&outboundaltsenabled=false&inboundaltsenabled=false"
            line_info = "從 "+originplace+" 到 "+destinationplace+" 的機票在 "+go_date_min+" 出發 "+leave_date_min+" 回程的機票目前最低價為: "+price_min+" 連結:"+link_min
            
            c_mod = conn.cursor()
            c_mod.execute("UPDATE RES set PRICE = "+price_min+" where ID="+task_id)
            c_mod.execute("UPDATE RES set GO_DATE = "+go_date_min+" where ID="+task_id)
            c_mod.execute("UPDATE RES set LEAVE_DATE = "+leave_date_min+" where ID="+task_id)
            conn.commit()
            
            line_send_link = "https://maker.ifttt.com/trigger/flight_price/with/key/"+ifttt_key+"?value1="+line_info
            requests.get(url=line_send_link)
    if flag == 0:
        link_min = "https://www.skyscanner.com.tw/transport/flights/"+originplace+"/"+destinationplace+"/"+go_date_min+"/"+leave_date_min+"/?adults=1&children=0&adultsv2=1&childrenv2=&infants=0&cabinclass=economy&rtn=1&preferdirects=false&outboundaltsenabled=false&inboundaltsenabled=false"
        line_info = "從 "+originplace+" 到 "+destinationplace+" 的機票在 "+go_date_min+" 出發 "+leave_date_min+" 回程的機票目前最低價為: "+price_min+" 連結:"+link_min
        c_ins = conn.cursor()
        c_ins.execute("INSERT INTO RES (ID,PRICE,GO_DATE,LEAVE_DATE) VALUES ("+str(task_id)+", '"+price_min+"','"+go_date_min+"','"+leave_date_min+"' )");
        conn.commit()
        line_send_link = "https://maker.ifttt.com/trigger/flight_price/with/key/"+ifttt_key+"?value1="+line_info
        requests.get(url=line_send_link)
    
conn.close()