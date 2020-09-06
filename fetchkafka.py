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

"""從未加認證的主機接數據"""
class Fetch(object) :
	def __init__(self) :
		self.config = configparser.RawConfigParser()
		self.config.read('./config.cfg')
		if not os.path.exists('./Log'):
			os.makedirs('./Log')
		logging.basicConfig(filename='./Log/'+datetime.datetime.today().strftime("%Y%m%d")+'.log'
			, level=logging.INFO
			, format='%(asctime)s %(message)s'
			, datefmt='%Y/%m/%d %I:%M:%S %p')
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

	def fetch_xxxx(self):
		try:
			onelist = []
			count = 0
			while True:
				msg = self.consumer.poll(1)
				if msg is None:
					continue
				if msg.error():
					print('Consumer error: {}'.format(msg.error()))
					continue
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