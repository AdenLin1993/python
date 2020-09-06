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
import pymysql
import logging
import textwrap
import time
import re
import zlib
import time
import json
import xlrd
import csv
from shutil import copyfile

class Push(object) :
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
			self.MySqlConn = pymysql.connect(host=self.config.get('MySQL','mysqlserver')
				,user=self.config.get('MySQL','mysqluser')
				,passwd=self.config.get('MySQL','mysqlpassword')
				,db=self.config.get('MySQL','database')
				,charset='utf8')
		except Exception as inst:
			print('MySql Connection Fail')
			print(inst)
			logging.error('MySql Connection Fail')
			logging.error(inst)
		
	def push_xxxx(self,df) :
		print('push data  Start')
		logging.info('push data Start')

		Createid = datetime.datetime.now()
		pushstatus = 0

		SqlHead = """replace into tranning5.npi_prinfo values"""
		SqlList = []
		SqlList.append(SqlHead)
		data = df.values.tolist()
		rowindex = 0
		while rowindex < len(data):
			BG = Push.convertdata(self,data[rowindex][0],'str')
			BU = Push.convertdata(self,data[rowindex][1],'str')
			SITE = Push.convertdata(self,data[rowindex][2],'str')
			PLANT = Push.convertdata(self,data[rowindex][3],'str')
			CUSTOMER = Push.convertdata(self,data[rowindex][4],'str')

			PROJECTCODE = Push.convertdata(self,data[rowindex][5],'str')
			PROJECTNAME = Push.convertdata(self,data[rowindex][6],'str')
			MODEL = Push.convertdata(self,data[rowindex][7],'str')
			STAGE = Push.convertdata(self,data[rowindex][8],'str')
			SUBSTAGE = Push.convertdata(self,data[rowindex][9],'str')

			PN = Push.convertdata(self,data[rowindex][10],'str')
			DESCRIPTION = Push.convertdata(self,data[rowindex][11],'str')
			RELATEDPN = Push.convertdata(self,data[rowindex][12],'str')
			CUSTOMERPN = Push.convertdata(self,data[rowindex][13],'str')
			PRTYPE = Push.convertdata(self,data[rowindex][14],'str')

			ENGVER = Push.convertdata(self,data[rowindex][15],'str')
			PCBVER = Push.convertdata(self,data[rowindex][16],'str')
			PANEL = Push.convertdata(self,data[rowindex][17],'int')
			GPCODE = Push.convertdata(self,data[rowindex][18],'str')
			PRSTARTDATE = Push.convertdata(self,data[rowindex][19],'datetime')

			STSTUS = Push.convertdata(self,data[rowindex][20],'str')
			PROCESSSTAGE = Push.convertdata(self,data[rowindex][21],'str')
			DEMANDHOUR = Push.convertdata(self,data[rowindex][22],'float')
			LINE = Push.convertdata(self,data[rowindex][23],'str')
			QUANTITY = Push.convertdata(self,data[rowindex][24],'int')

			SHIPMENTALLOCATION = Push.convertdata(self,data[rowindex][25],'str')
			MODELTYPE = Push.convertdata(self,data[rowindex][26],'str')
			MFGLEVEL = Push.convertdata(self,data[rowindex][27],'str')
			CreateId = Createid

			SqlList.append("""(
				{0},{1},{2},{3},{4},
				{5},{6},{7},{8},{9},
				{10},{11},{12},{13},{14},
				{15},{16},{17},{18},{19},
				{20},{21},{22},{23},{24},
				{25},{26},{27},{28}
				)""".format(
					"'{0}'".format(BG)
					,"'{0}'".format(BU)
					,"'{0}'".format(SITE)
					,"'{0}'".format(PLANT)
					,"'{0}'".format(CUSTOMER)
					
					,"'{0}'".format(PROJECTCODE)
					,"'{0}'".format(PROJECTNAME)
					,"'{0}'".format(MODEL)
					,"'{0}'".format(STAGE)
					,"'{0}'".format(SUBSTAGE)					

					,"'{0}'".format(PN)
					,"'{0}'".format(DESCRIPTION)
					,"'{0}'".format(RELATEDPN)
					,"'{0}'".format(CUSTOMERPN)
					,"'{0}'".format(PRTYPE)

					,"'{0}'".format(ENGVER)
					,"'{0}'".format(PCBVER)
					,"'{0}'".format(PANEL)
					,"'{0}'".format(GPCODE)
					,"'{0}'".format(PRSTARTDATE)

					,"'{0}'".format(STSTUS)
					,"'{0}'".format(PROCESSSTAGE)
					,"'{0}'".format(DEMANDHOUR)
					,"'{0}'".format(LINE)
					,"'{0}'".format(QUANTITY)

					,"'{0}'".format(SHIPMENTALLOCATION)
					,"'{0}'".format(MODELTYPE)
					,"'{0}'".format(MFGLEVEL)
					,"'{0}'".format(CreateId)
			))
			SqlList.append(",\n")
			rowindex += 1

			if rowindex % 10000 == 0:
				SqlList.pop()
				SqlList.append(';')
				SqlBuffer = ''.join(SqlList)
				try :
					Cur = self.MySqlConn.cursor()
					Cur.execute(SqlBuffer)
					self.MySqlConn.commit()
					Cur.close()
				except Exception as inst:
					print('replace mysql fail')
					print(inst)
					pushstatus = pushstatus + 1
					with codecs.open('./Sql/Err.sql','wb','utf-8') as f:
						f.write(SqlBuffer)
				SqlList = []
				SqlList.append("""replace into tranning5.npi_prinfo values""")

		if len(SqlList) > 1 :
			SqlList.pop()
			SqlList.append(';')
			SqlBuffer = ''.join(SqlList)
			try :
				Cur = self.MySqlConn.cursor()
				Cur.execute(SqlBuffer)
				self.MySqlConn.commit()
				Cur.close()
			except Exception as inst:
				print('replace mysql fail')
				print(inst)
				pushstatus = pushstatus + 1
				with codecs.open('./Sql/Err.sql','wb','utf-8') as f:
					f.write(SqlBuffer)

		try :
			with self.MySqlConn.cursor() as Cur :
				Sqldelete = """DELETE FROM tranning5.npi_prinfo where CreateId <> '{}';""".format(Createid)
				Cur.execute(Sqldelete)
				self.MySqlConn.commit()
		except Exception as inst:
			print('Delete dat_qis.npi_prinfo fail')
			print(inst)
			with codecs.open('./Sql/Err.sql','wb','utf-8') as f:
				f.write(Sqldelete)

		print('push data Finish')
		logging.info('push data Finish')

		print(pushstatus)
		return pushstatus

	def convertdata(self,data,datatype) :
		try :
			result = data
			if datatype == 'float' :
				if isinstance(result, float) :
					return result
				if result is None :
					return 0.0
				try :
					return float(result)
				except ValueError :
					return 0.0
			if  datatype == 'int':
				if isinstance(result, int) :
					return result
				if result is None :
					return 0
				try :
					return int(result)
				except ValueError :
					return 0
			if datatype == 'str':
				if isinstance(result, str) :
					result = result.strip()
					if result.find('\'') >= 0 :
						result = result.replace('\'',"\\\'")
					elif result.find('\\') >= 0 :
						result = result.replace('\\',"\\\\")
					return result
				if result is None :
					result = "null"
					return result
				try :
					return str(result)
				except ValueError :
					return "null"

			if datatype == 'datetime' :
				if result is None :
					return "0000-00-00 00:00:00"
				else :
					result = data
					return result

		except Exception as inst:
			print("convertdata Error")
			print(result)
			print(inst)

		return result

	def formatdata(self,data) :
		result = data
		if isinstance(result, str) :
			result = result.replace('{','(')
			result = result.replace('}',')')
			result = result.replace('（','(')
			result = result.replace('）',')')
			goldpat = "[Y]\(SB[\d_-]+?\)|SB[\d_-]+\d"
			pats = re.findall(goldpat,result)
			pats = list(set(pats))
			patdict = {}
			for pat in pats :
				patvalue = pat.replace(pat,'sbmeter["{}"]'.format(pat))
				patdict[pat] = patvalue
			pattern = re.compile(goldpat)
			result = pattern.sub(lambda m : patdict[m.group(0)],result)

		return result
