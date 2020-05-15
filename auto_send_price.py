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

    url = "https://" + rapidapi_host + "/apiservices/browseroutes/v1.0/" + country + "/" + currency + "/" + locale + "/" + originplace + "-sky/" + destinationplace + "-sky/" + outboundpartialdate + "/" + inboundpartialdate

    response = requests.request("GET", url, headers=get_header)
    res = response.json()

    if len(pd.DataFrame(res['Quotes'])) != 0:

        outbound_leg_list = pd.DataFrame(res['Quotes'])['OutboundLeg']
        inbound_leg_list = pd.DataFrame(res['Quotes'])['InboundLeg']
        min_price = pd.DataFrame(res['Quotes'])['MinPrice']

        carrierid_filter = list()

        df_outbound_leg = pd.DataFrame(outbound_leg_list[0])
        if df_outbound_leg["CarrierIds"][0] in excludeCarriers:
            carrierid_filter = carrierid_filter.append(0)

        i = 1
        while i < len(outbound_leg_list):
            tmp = pd.DataFrame(outbound_leg_list[i])
            if tmp["CarrierIds"][0] in excludeCarriers:
                if i not in carrierid_filter:
                    carrierid_filter = carrierid_filter.append(i)
            df_outbound_leg = df_outbound_leg.append(tmp)
            i += 1
        df_outbound_leg = df_outbound_leg.reset_index(drop=True)

        df_inbound_leg = pd.DataFrame(inbound_leg_list[0])
        i = 1
        while i < len(inbound_leg_list):
            tmp = pd.DataFrame(inbound_leg_list[i])
            if tmp["CarrierIds"][0] in excludeCarriers:
                if i not in carrierid_filter:
                    carrierid_filter = carrierid_filter.append(i)
            df_inbound_leg = df_inbound_leg.append(tmp)
            i += 1
        df_inbound_leg = df_inbound_leg.reset_index(drop=True)

        for drop_index in carrierid_filter:
            min_price.drop(drop_index)

        price_sorted = min_price.sort_values()
        lowest_price_index = price_sorted.index[0]

        price_min = int(price_sorted.iloc[0])
        go_date_min = datetime.strptime(df_outbound_leg.loc[lowest_price_index]["DepartureDate"],
                                        "%Y-%m-%dT00:00:00").strftime("%y%m%d")
        leave_date_min = datetime.strptime(df_inbound_leg.loc[lowest_price_index]["DepartureDate"],
                                           "%Y-%m-%dT00:00:00").strftime("%y%m%d")

        go_date_min_text = datetime.strptime(df_outbound_leg.loc[lowest_price_index]["DepartureDate"],
                                             "%Y-%m-%dT00:00:00").strftime("%Y-%m-%d")
        leave_date_min_text = datetime.strptime(df_inbound_leg.loc[lowest_price_index]["DepartureDate"],
                                                "%Y-%m-%dT00:00:00").strftime("%Y-%m-%d")

        c_res = conn.cursor()
        cursor_res = c_res.execute("SELECT * from RES where ID=" + str(task_id))
        res_db = cursor_res.fetchall()

        link_min = "https://www.skyscanner.com.tw/transport/flights/" + originplace + "/" + destinationplace + "/" + go_date_min + "/" + leave_date_min + "/?adults=1&children=0&adultsv2=1&childrenv2=&infants=0&cabinclass=economy&rtn=1&preferdirects=false&outboundaltsenabled=false&inboundaltsenabled=false"

        if len(res_db) != 0:
            print("Previous price = " + str(int(res_db[0][1])) + " Now price = " + str(int(float(price_min))))

            line_info = "從" + originplace + "到" + destinationplace + "在" + go_date_min_text + "出發" + leave_date_min_text + "回程  最低價為: " + str(
                price_min) + " 前次價格為: " + str(int(res_db[0][1])) + " skyscanner:" + link_min

            c_mod = conn.cursor()
            c_mod.execute("UPDATE RES set PRICE = " + str(price_min) + " where ID=" + str(task_id))
            c_mod.execute("UPDATE RES set GO_DATE = " + go_date_min + " where ID=" + str(task_id))
            c_mod.execute("UPDATE RES set LEAVE_DATE = " + leave_date_min + " where ID=" + str(task_id))
            conn.commit()

        else:
            line_info = "從" + originplace + "到" + destinationplace + "在" + go_date_min_text + "出發" + leave_date_min_text + "回程  最低價為: " + str(
                price_min) + " skyscanner:" + link_min

            c_ins = conn.cursor()
            c_ins.execute("INSERT INTO RES (ID,PRICE,GO_DATE,LEAVE_DATE) VALUES (" + str(
                task_id) + ", '" + price_min + "','" + go_date_min + "','" + leave_date_min + "' )");
            conn.commit()

        line_send_link = "https://maker.ifttt.com/trigger/flight_price/with/key/" + ifttt_key + "?value1=" + line_info
        requests.get(url=line_send_link)

conn.close()
