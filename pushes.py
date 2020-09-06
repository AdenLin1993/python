#!/usr//bin/python
#-*- coding: utf-8 -*-

import os										
import sys										
import io
import time											
import datetime										
import configparser								
import logging										
import re											
import copy										
from elasticsearch import Elasticsearch
from elasticsearch import helpers				

class Push(object) :
	def __init__(self) :
		# self.Index = ["wzs_dat_aircomp_eer_2020-08"]
		
		if not os.path.exists('./Log') :
			os.makedirs('./Log')
		logging.basicConfig(filename='./Log/'+datetime.datetime.today().strftime("%Y%m%d")+'.log'
			, level=logging.INFO
			, format='%(asctime)s %(message)s'
			, datefmt='%Y/%m/%d %I:%M:%S %p')
		try :
			self.EsClient = Elasticsearch(["zsarm-emnrdb-p01.wzs.wistron","zsarm-emnrdb-p02.wzs.wistron","zsarm-emnrdb-p03.wzs.wistron"],maxsize = 25,timeout = 180)
		except Exception as inst :
			print('Elasticsearch connect fail')
			print(inst)

	def push_aircomp_demo_data(self):
		onemessage = {'site': 'WZS', 'pub_by': 'DAT','building': 'TB2', 'project': 'air_comp', 'insert_evt_dt': 1597802922000, 'Target_Power': 1415.96, 'Actual_Power': 1347.4, 'Energy_Saving': 68.56, 'Energy_Saving_Rate': 4.84, 'Actual_Volume': 10931.84, 'Actual_Pressure': 6.64, 'Target_Volume': 8855.79, 'Target_Pressure': 'NA', 'evt_dt': 1597802400000}

		action = {'_op_type':'index',  
            '_index':'wzs_dat_aircomp_eer_2020-08',
            '_type':'energy',
            '_source':onemessage}
		actions = []
		actions.append(action)
		helpers.bulk(client = self.EsClient,actions = actions)

		# action = {'_op_type':'delete',  
        #     '_index':'wzs_dat_aircomp_eer_2020-08',
        #     '_type':'energy',
		# 	'_id': 'AXQuaH0IHQXd2710v61Z',
        #     '_source':onemessage}
		# actions = []
		# actions.append(action)
		# helpers.bulk(client = self.EsClient,actions = actions)

push = Push()
push.push_aircomp_demo_data()
		