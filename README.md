# 自動推播機票最低價格
利用skyscanner的機票價格查詢API，並藉由ifttt將查詢的結果推播至LINE

如果搭配Linux的crontab可以達到每日自動推送功能
##
### 準備API key
首先需要先申請這兩個服務的API key
###### skyscanner API : https://rapidapi.com/skyscanner/api/skyscanner-flight-search
###### LINE : https://ifttt.com/
##
### 初始化DB
可以利用 DB Browser for SQLite 將API key與預計查詢的地點、日期填入DB
###### DB Browser for SQLite : https://sqlitebrowser.org/
##  
### flight.db
內部有3個不同的Table，分別為API、RES、TASK

#### API
在skyscanner內填入rapidapi的API key，ifttt的則填入ifttt的key
#### RES
這Table內會儲存上一次執行時的最低價、去程日期與回程日期，ID則對應到TASK的ID
#### TASK
這邊則是存放不同的查詢任務

ORI_PLACE : 出發地

DES_PLACE : 目的地

OUT_DATE : 出發日期

IN_DATE : 回程日期

MAX_STOP : 最大中轉次數 (但目前因為API改版，這參數已沒有實際用途)
##
### 成果
最後在LINE所接收到的訊息如下圖，其中的連結能直接連回到Skyscanner進行購買


![Imgur](https://i.imgur.com/cdbquJd.png)
