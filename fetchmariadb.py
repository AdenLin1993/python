#!/usr/bin/python
#-*- coding: utf-8 -*-

import os
import sys
import configparser
import datetime
import urllib.request
import urllib
from urllib.parse import quote_plus,quote
from urllib.request import urlopen
import json
import codecs
import io
import time
import re
import pymysql
from sqlalchemy import create_engine
import logging
import textwrap
import pandas as pd

"""第一種方法"""
class Fetch_pymysql(object) :
	def __init__(self) :
		self.config = configparser.RawConfigParser()
		self.config.read('./config.cfg')
		if not os.path.exists('./Log'):
			os.makedirs('./Log')
		if not os.path.exists('./Sql') :
			os.makedirs('./Sql')
		logging.basicConfig(filename='./Log/'+datetime.datetime.today().strftime("%Y%m%d")+'.log'
			, level=logging.INFO
			, format='%(asctime)s %(message)s'
			, datefmt='%Y/%m/%d %I:%M:%S %p')
		try :
			self.MySqlConn = pymysql.connect(host=self.config.get('ZS_IT_MySQLR','mysqlserver')
				,user=self.config.get('ZS_IT_MySQLR','mysqluser')
				,passwd=self.config.get('ZS_IT_MySQLR','mysqlpassword')
				,db=self.config.get('ZS_IT_MySQLR','database')
				,charset='utf8')
		except Exception as inst:
			print('MySql Connection Fail')
			print(inst)
			logging.error('MySql Connection Fail')
			logging.error(inst)

	def fetch_xxxx(self):
		try:
			with self.MySqlConn.cursor() as cursor:
				sql = """SELECT * 
				FROM dat_energy.wzs_sb_base_data 
				WHERE wzs_sb_base_data.batchid = 
				(SELECT MAX(wzs_sb_base_data.batchid) 
				FROM dat_energy.wzs_sb_base_data) 
				;"""
				cursor.execute(sql)
				meterbase = cursor.fetchall()
		except Exception as inst:
				print('maridb fetch meterbase Fail')
				print(inst)
				logging.error('happen maridb fecth meterbase Fail')
				logging.error(inst)
		return meterbase

"""第二種方法"""
class Fetch_sqlalchemy(object) :
	def __init__(self) :
		self.config = configparser.RawConfigParser()
		self.config.read('./config.cfg')
		if not os.path.exists('./Log'):
			os.makedirs('./Log')
		if not os.path.exists('./Sql') :
			os.makedirs('./Sql')
		logging.basicConfig(filename='./Log/'+datetime.datetime.today().strftime("%Y%m%d")+'.log'
			, level=logging.INFO
			, format='%(asctime)s %(message)s'
			, datefmt='%Y/%m/%d %I:%M:%S %p')

		self.host = self.config.get('ZS_IT_MySQLW','mysqlserver')
		self.user = self.config.get('ZS_IT_MySQLW','mysqluser')
		self.pwd = self.config.get('ZS_IT_MySQLW','mysqlpassword')
		self.db = self.config.get('ZS_IT_MySQLW','database')
		try:
			sqlEngine = create_engine("mysql+pymysql://{0}:{1}@{2}:3306/{3}?charset=utf8".format(self.user,self.pwd,self.host,self.db))
			self.dbConnection = sqlEngine.connect()
		except Exception as inst:
			print('MySql Connection Fail')
			print(inst)
			logging.error('MySql Connection Fail')
			logging.error(inst)

	def fetch_xxxx(self):
		try:
			sql = "SELECT * FROM dat_energy.wzs_sb_base_data WHERE wzs_sb_base_data.batchid = (SELECT MAX(wzs_sb_base_data.batchid) FROM dat_energy.wzs_sb_base_data); "
			meterbase_df = pd.read_sql(sql,self.dbConnection)
		except Exception as inst:
				print('maridb fetch meterbase Fail')
				print(inst)
				logging.error('happen maridb fecth meterbase Fail')
				logging.error(inst)
		print(meterbase)
		self.dbConnection.close()
		return meterbase_df
