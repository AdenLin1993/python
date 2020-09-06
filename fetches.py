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

class Fetch(object) :
	def __init__(self) :
		self.Index = ["fem_meterreading_*"]
		
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

	def meter_hour_cur(self,cur_flagtime):
		print('fetch {0} Start'.format(self.Index))
		logging.info('fetch {0} Start'.format(self.Index))

		""" comfirm start_time and end_time """
		Cur_StartDate_timestamp = (cur_flagtime-3600)*1000
		Cur_EndDate_timestamp = cur_flagtime*1000

		""" get cur hour reading  """
		query_body = {
			"query": {
				"bool" : {
					"must" : [
						{"range" :{
								"evt_dt" : {
									"from" : 0,"to" : 0,"include_lower" : True,"include_upper":True
								}
							}
						},
						{
							"match_phrase": {
								"type": {
									"query": "SB",
									"slop": 0,
									"boost": 1
								}
							}
						}
					]
				}
			}
		}
		
		sbmeter_end = {}
		while Cur_StartDate_timestamp < Cur_EndDate_timestamp :
			hour_Cur_StartDate_timestamp = Cur_StartDate_timestamp
			hour_Cur_EndDate_timestamp = hour_Cur_StartDate_timestamp + 3600000

			query_body['query']['bool']['must'][0]['range']['evt_dt']['from'] = hour_Cur_StartDate_timestamp
			query_body['query']['bool']['must'][0]['range']['evt_dt']['to'] = hour_Cur_EndDate_timestamp
			page = self.EsClient.search(index=self.Index, size=100000, body=query_body, scroll='50m')

			for oneHit in page["hits"]["hits"] :
				"""如果篩選的內容中有異常數據，跳過處理"""
				try :
					insertObj = copy.copy(oneHit['_source'])
					Key = "{0}_{1}_{2}".format(insertObj['site'],insertObj['building'],insertObj['meterId'])

					""" get the end data """
					if sbmeter_end.get(Key,None) is None :
						sbmeter_end[Key] = insertObj
					else :
						if int(insertObj['evt_dt']) >= int(sbmeter_end[Key]['evt_dt']) :
							sbmeter_end[Key] = insertObj
				except Exception as inst :
					print(inst)
								
			Cur_StartDate_timestamp = Cur_StartDate_timestamp + 3600000

		print("The hour data quantity",len(sbmeter_end))
				
		print('{0} fetch Finish'.format(self.Index))
		logging.info('{0} fetch Finish'.format(self.Index))

		return sbmeter_end