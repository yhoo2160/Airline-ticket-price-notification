# -*- coding: UTF-8 -*-
import requests
import sqlite3
from time import sleep
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
adults = 4
rapidapi_host = "skyscanner-skyscanner-flight-search-v1.p.rapidapi.com"
get_header = {'x-rapidapi-host' : rapidapi_host,
              'x-rapidapi-key': skyscanner_key,
              'content-type': "application/x-www-form-urlencoded"}

excludeCarriers = [1085,1429,1183]
excludeAgents = [2370279,3589095,3934681,3987687,3934928,2045415,2185959,1963108,4499175,2741223,]

cursor = c.execute("SELECT * from TASK")
for row in cursor:
    print(row)
    task_id = row[0]
    originplace = row[1]
    destinationplace = row[2]
    outboundpartialdate = row[3]
    inboundpartialdate = row[4]
    maxstop = row[5] + 1

    url = "https://" + rapidapi_host + "/apiservices/pricing/v1.0"
    payload = "inboundDate=" + inboundpartialdate + "&cabinClass=economy&children=0&infants=0&country=" + country + "&currency=" + currency + "&locale=" + locale + "&originPlace=" + originplace + "-sky&destinationPlace=" + destinationplace + "-sky&outboundDate=" + outboundpartialdate + "&adults=" + str(
        adults)
    response = requests.request("POST", url, data=payload, headers=headers)
    print(response)
    print(response.headers['location'].split("/")[-1])

    sleep(30)

    url = "https://" + rapidapi_host + "/apiservices/pricing/uk2/v1.0/" + response.headers['location'].split("/")[-1]
    querystring = {"sortType": "price", "sortOrder": "asc", "pageSize": "100"}
    response = requests.request("GET", url, headers=headers, params=querystring)

    res = response.json()
    agents_list = pd.DataFrame(res['Agents']).drop_duplicates('Id').set_index('Id')
    leg_list = pd.DataFrame(res['Legs']).drop_duplicates('Id').set_index('Id')
    carriers_list = pd.DataFrame(res['Carriers']).drop_duplicates('Id').set_index('Id')

    i = 0
    res_list = list()
    while i < len(res['Itineraries']):
        if any(leg_list.loc[res['Itineraries'][i]['OutboundLegId']]['OperatingCarriers']) in excludeCarriers:
            pass
        elif any(leg_list.loc[res['Itineraries'][i]['InboundLegId']]['OperatingCarriers']) in excludeCarriers:
            pass
        elif len(leg_list.loc[res['Itineraries'][i]['OutboundLegId']]['Stops']) >= maxstop:
            # print((leg_list.loc[res['Itineraries'][i]['OutboundLegId']]['Stops']))
            pass
        elif len(leg_list.loc[res['Itineraries'][i]['InboundLegId']]['Stops']) >= maxstop:
            # print((leg_list.loc[res['Itineraries'][i]['InboundLegId']]['Stops']))
            pass
        else:
            j = 0
            while j < len(res['Itineraries'][i]['PricingOptions']):
                if res['Itineraries'][i]['PricingOptions'][j]['Agents'][0] in excludeAgents:
                    pass
                else:
                    direct_link = requests.request("GET", "https://tinyurl.com/api-create.php?" +
                                                   res['Itineraries'][i]['PricingOptions'][j]['DeeplinkUrl']).text
                    res_list.append([agents_list.loc[res['Itineraries'][i]['PricingOptions'][j]['Agents'][0]]['Name'],
                                     res['Itineraries'][i]['PricingOptions'][j]['Price'],
                                     direct_link,
                                     leg_list.loc[res['Itineraries'][i]['OutboundLegId']]['Departure'].split('T')[0],
                                     leg_list.loc[res['Itineraries'][i]['InboundLegId']]['Departure'].split('T')[0]])
                j = j + 1
        i = i + 1

    df = pd.DataFrame(res_list)

    agent_min = df[df[1] == df[1].min()][0].iloc[-1]
    price_min = str(int(df[df[1] == df[1].min()][1].iloc[-1]))
    print(price_min)
    go_date_min = df[df[1] == df[1].min()][3].iloc[-1]
    leave_date_min = df[df[1] == df[1].min()][4].iloc[-1]
    direct_link = df[df[1] == df[1].min()][2].iloc[-1]

    c_res = conn.cursor()
    cursor_res = c_res.execute("SELECT * from RES where ID=" + str(task_id))
    flag = 0
    for row in cursor_res:
        flag = 1
        print("Previous price = " + str(int(row[1])) + " Now price = " + str(int(float(price_min))))
        if int(row[1]) != int(float(price_min)):
            link_min = "https://www.skyscanner.com.tw/transport/flights/" + originplace + "/" + destinationplace + "/" + go_date_min + "/" + leave_date_min + "/?adults=1&children=0&adultsv2=1&childrenv2=&infants=0&cabinclass=economy&rtn=1&preferdirects=false&outboundaltsenabled=false&inboundaltsenabled=false"
            line_info = "從" + originplace + "到" + destinationplace + "在" + go_date_min + "出發" + leave_date_min + "回程  最低價為: " + price_min + "(" + agent_min + ")" + " 前次價格為: " + str(
                row[1]) + " 訂票連結:" + direct_link + " skyscanner:" + link_min

            c_mod = conn.cursor()
            c_mod.execute("UPDATE RES set PRICE = " + str(price_min) + " where ID=" + str(task_id))
            c_mod.execute("UPDATE RES set GO_DATE = " + go_date_min + " where ID=" + str(task_id))
            c_mod.execute("UPDATE RES set LEAVE_DATE = " + leave_date_min + " where ID=" + str(task_id))
            conn.commit()

            line_send_link = "https://maker.ifttt.com/trigger/flight_price/with/key/" + ifttt_key + "?value1=" + line_info
            requests.get(url=line_send_link)
    if flag == 0:
        link_min = "https://www.skyscanner.com.tw/transport/flights/" + originplace + "/" + destinationplace + "/" + go_date_min + "/" + leave_date_min + "/?adults=1&children=0&adultsv2=1&childrenv2=&infants=0&cabinclass=economy&rtn=1&preferdirects=false&outboundaltsenabled=false&inboundaltsenabled=false"
        line_info = "從" + originplace + "到" + destinationplace + "在" + go_date_min + "出發" + leave_date_min + "回程  最低價為: " + price_min + "(" + agent_min + ")" + " 訂票連結:" + direct_link + " skyscanner:" + link_min
        c_ins = conn.cursor()
        c_ins.execute("INSERT INTO RES (ID,PRICE,GO_DATE,LEAVE_DATE) VALUES (" + str(
            task_id) + ", '" + price_min + "','" + go_date_min + "','" + leave_date_min + "' )");
        conn.commit()
        line_send_link = "https://maker.ifttt.com/trigger/flight_price/with/key/" + ifttt_key + "?value1=" + line_info
        requests.get(url=line_send_link)

conn.close()
