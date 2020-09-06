#!/usr/bin/python
#-*- coding: utf-8 -*-

import os
import sys							
import io							
import time							
import datetime						
import configparser					
import logging						
import codecs						
import re							
import pymysql						
import paho.mqtt.publish as publish
import fetchtgvlm
import fetchmaridb
import fetches
import volume_prediction_auto

""" time control"""
CUR = int(time.time())
CUR_TIME = CUR*1000
CUR_TIME2 = (CUR-CUR%3600)*1000

""" Create a class that fetch data from mariadb"""
fetchmaridb1 = fetchmaridb.Fetch()

""" Create a class that fetch data from ES"""
fetches1 = fetches.Fetch()

"""1、Target_Power"""
TARGET_POWER_LIST = fetchmaridb1._target_power_info()
if len(TARGET_POWER_LIST) > 0 and TARGET_POWER_LIST[0][1] is not None :
    TARGET_POWER = round(TARGET_POWER_LIST[0][1],2)
else :
    TARGET_POWER = 0

"""2、Actual_Power"""
ACTUAL_POWER_LIST = fetchmaridb1._actual_power_info()
if len(ACTUAL_POWER_LIST) > 0 and ACTUAL_POWER_LIST[0][1] is not None :
    ACTUAL_POWER = round(ACTUAL_POWER_LIST[0][1],2)
else :
    ACTUAL_POWER = 0

"""3、Energy_Saving"""
"""4、Energy_Saving_Rate"""
if TARGET_POWER != 0 and ACTUAL_POWER != 0 :
    ENERGY_SAVING = round(TARGET_POWER - ACTUAL_POWER,2)
    ENERGY_SAVING_RATE = round((ENERGY_SAVING/TARGET_POWER)*100,2)
else :
    ENERGY_SAVING = 0
    ENERGY_SAVING_RATE = 0

""" 5、Actual_Volume"""
ACTUAL_VOLUME_LIST = fetchmaridb1._aircomp_eer_info()

ACTUAL_VOLUME = 0
for onedata in ACTUAL_VOLUME_LIST :
    airmeter = onedata[0]
    meterid = int(airmeter.split('_')[2])
    if meterid <= 16 :
        if onedata[1] > 725 :
            airreading_gap = 725
        else :
            airreading_gap = onedata[1]
    if meterid > 16 :
        if onedata[1] > 1260 :
            airreading_gap = 1260
        else :
            airreading_gap = onedata[1]
    ACTUAL_VOLUME = ACTUAL_VOLUME + airreading_gap
ACTUAL_VOLUME = round(ACTUAL_VOLUME,2)

""" 6、Actual_Pressure"""
ACTUAL_PRESSURE = round(fetches1._fetch_pressure_avg(CUR_TIME2),2)

""" 7、Target_Volume"""
volume_prediction_auto.fetch_x_test(CUR_TIME2)
TARGET_VOLUME = round(fetchtgvlm.target_volume(),2)

""" 8、Target_Pressure"""
TARGET_PRESSURE = 'NA'

# demo message
result = {
    "site": "WZS", 
    "pub_by": "DA", 
    "api": "Energy", 
    "building": "TB2", 
    "project":"air_comp",
    "insert_evt_dt": 0, 
    "data": {"type" : "power", "Target_Power": 0, "Actual_Power": 0, 
    "Energy_Saving": 0, "Energy_Saving_Rate": 0, "Actual_Volume": 0, 
    "Actual_Pressure": 0, "Target_Volume": 0, "Target_Pressure": 0, 
    "evt_dt": 0}
}

result["insert_evt_dt"] = CUR_TIME
result["data"]["Target_Power"] = TARGET_POWER
result["data"]["Actual_Power"] = ACTUAL_POWER
result["data"]["Energy_Saving"] = ENERGY_SAVING
result["data"]["Energy_Saving_Rate"] = ENERGY_SAVING_RATE
result["data"]["Actual_Volume"] = ACTUAL_VOLUME
result["data"]["Actual_Pressure"] = ACTUAL_PRESSURE
result["data"]["Target_Volume"] = TARGET_VOLUME
result["data"]["Target_Pressure"] = TARGET_PRESSURE
result["data"]["evt_dt"] = CUR_TIME2

# merge all message ex:one message
msgs = []

msg = {'topic':0, 'payload':0, 'retain':True}   #package one message
msg['topic'] = "WZS/DA/Energy/TB2/Aircomp/power"
msg['payload'] = str(result)

msgs.append(msg)

# push all message to mqtt server
try:
    publish.multiple(msgs,hostname="zsarm-mqtt-p03.wzs.wistron",port=9001,client_id="carey_lin", keepalive=60,transport="websockets")
except Exception as inst :
	print("mqtt connect fail ！")


