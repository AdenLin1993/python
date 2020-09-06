import os
import logging
import datetime
import time
import pymysql
import configparser
from confluent_kafka import KafkaError,Consumer,KafkaException
from bson import json_util, ObjectId
from bson.json_util import dumps,loads,JSONOptions,DEFAULT_JSON_OPTIONS
import pandas as pd
import numpy as np


""" 创建一个从KAFKA数据库拉取数据的类"""
"""從未加認證的主機接數據"""
class Fetch(object) :
	"""重写类的初始化属性"""
	def __init__(self) :
		self.config = configparser.RawConfigParser()
		self.config.read('./config.cfg')
		if not os.path.exists('./Log'):
			os.makedirs('./Log')
		logging.basicConfig(filename='./Log/'+datetime.datetime.today().strftime("%Y%m%d")+'.log'
			, level=logging.INFO
			, format='%(asctime)s %(message)s'
			, datefmt='%Y/%m/%d %I:%M:%S %p')
		
		"""建立与kafka数据库连接的客户端，group.id随意命名，保证其他人没有在使用这个id，因为一个id只能拉取一次数据，每次拉取数据都拉取七天前到现在的数据"""
		try :
			Source_Kafka_Consumer = Consumer({
					'bootstrap.servers':'10.41.241.6:9092'
					,'group.id':'wzs.pmo.p3.careyfetch'
					,'auto.offset.reset':'earliest'
					, 'session.timeout.ms': 6000
					})
			DEFAULT_JSON_OPTIONS.strict_uuid = True
			Source_Kafka_Consumer.subscribe(['wzs.wigps.seccard'])
			self.consumer = Source_Kafka_Consumer
		except Exception as inst:
			print('kafaka Connection Fail')
			print(inst)
			logging.error('kafaka Connection Fail')
			logging.error(inst)
	
	"""这是一个从kafka数据库拉取数据的函数范例"""
	def fetch_xxxx(self):
		try:
			onelist = []
			count = 0
			while True:
				"""通过创建好连接kafka客服端和相关参数，拉取数据"""
				msg = self.consumer.poll(1)
				if msg is None:
					continue
				if msg.error():
					print('Consumer error: {}'.format(msg.error()))
					continue
				"""取出数据，和存储数据"""
				data = json_util.loads(msg.value())
				onelist.append(data)
				count = count + 1
				if count == 1000 :
					break
			self.consumer.close()
		except Exception as inst:
				print('kafaka fetch meterbase Fail')
				print(inst)
				logging.error('happen kafaka fecth meterbase Fail')
				logging.error(inst)
		return onelist
