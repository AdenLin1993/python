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
import pandas as pd					
import xlsxwriter
import xlrd

""" 创建一个从本地拉取excel指定特征工作簿的类 """
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

	def findpath(self,locatedri,filetpye,filestring = "."):
		print('findpath {0} Start'.format(filestring))
		logging.info('findpath {0} Start'.format(filestring))
		TotalFiles = 0
		SuccessFiles = 0
		for root, _, files in os.walk('{}'.format(locatedri)): 
			for onefile in files:
				try :
					filecompile = re.compile('{}'.format(filestring))
					findlength = len(filecompile.findall(onefile))
					if onefile.lower().endswith('.{}'.format(filetpye)) and findlength > 0:
						TotalFiles+= 1
						filepath = os.path.join(root, onefile)
						SuccessFiles+= 1
				except Exception as inst:
					print("find filepath fail")
					print(inst)
		print("Total Number of Files: "+ str(TotalFiles) +"; Success Files: " +str(SuccessFiles))
		logging.info("Total Number of Files: "+ str(TotalFiles) +"; Success Files: " +str(SuccessFiles))

		return filepath

	def getdataframe(self,filepath,sheetname) :
		print('getdataframe {0} Start'.format(sheetname))
		logging.info('getdataframe {0} Start'.format(sheetname))

		excel_reader = pd.ExcelFile(filepath)
		sheet_names = excel_reader.sheet_names
		filecompile = re.compile('{}'.format(sheetname))
		targetsheet = []
		for i in range(len(sheet_names)) :
			findlength = len(filecompile.findall(sheet_names[i].strip().upper()))
			if findlength > 0 :
				targetsheet.append(i)
		df = pd.read_excel(filepath,targetsheet[0],header = 0)
		newcol_name = ['No','meterid','MeterType','is_calculation','device_calculation','Spec','factory','building','Plant1','Plant2','share',
						'consum_type','area','floor','group','line','pd_line_meter','device','line_area','calculation_desc']
		df.columns = newcol_name

		return df
