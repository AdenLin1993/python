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
from ftplib import FTP				

class Fetch(object):
	def __init__(self) :
		self.config = configparser.RawConfigParser()
		self.config.read('./config.cfg')
		ftphost=self.config.get('P3SMTDIP_FTP','ftphost')
		ftpport=int(self.config.get('P3SMTDIP_FTP','ftpport'))
		ftpuser=self.config.get('P3SMTDIP_FTP','ftpuser')
		ftppw=self.config.get('P3SMTDIP_FTP','ftppw')

		if not os.path.exists('./Log') :
			os.makedirs('./Log')
		if not os.path.exists('./temporaryfile') :
			os.makedirs('./temporaryfile')
		logging.basicConfig(filename='./Log/'+datetime.datetime.today().strftime("%Y%m%d")+'.log'
			, level=logging.INFO
			, format='%(asctime)s %(message)s'
			, datefmt='%Y/%m/%d %I:%M:%S %p')
		try :
			self.FtpClient = FTP()
			self.FtpClient.connect(ftphost,ftpport)
			self.FtpClient.login(ftpuser,ftppw)
		except Exception as inst :
			print('Ftp Connect Fail')
			print(inst)
			logging.error('FTP Connection Fail')
			logging.error(inst)

	"""demoformat:fetch_xxxx("/Man_Power/Training_Roadmap","xlsx",filestring = "DLA")"""
	def fetch_xxxx(self,remotedri,filetpye,filestring = ".") :
		print('Start Download {0}'.format(remotedri))
		logging.info('Start Download {0}'.format(remotedri))

		self.FtpClient.cwd(remotedri)
		files = self.FtpClient.nlst()
		TotalFiles = 0
		SuccessFiles = 0
		for onefile in files :
			try :
				filecompile = re.compile('{}'.format(filestring))
				findlength = len(filecompile.findall(onefile))
				if onefile.lower().endswith('.{}'.format(filetpye)) and findlength > 0:
					TotalFiles+= 1
					TmpPath = './/temporaryfile//{0}'.format(onefile)
					self.FtpClient.retrbinary("RETR "+onefile,open(TmpPath,'wb').write)
					SuccessFiles+= 1
			except Exception as inst :
				print('Download {0} Fail'.format(onefile))
				print(inst)

		print("ftp downloadTotal Number of Files: "+ str(TotalFiles) +"; Success Files: " +str(SuccessFiles))
		logging.info("ftp downloadTotal Number of Files: "+ str(TotalFiles) +"; Success Files: " +str(SuccessFiles))
		self.FtpClient.quit()

"""根據文檔日期進行篩選下載，將邏輯加在第54行"""
# onefiletime =datetime.datetime.strptime(onefile.split('_')[2][:8],'%Y%m%d')
# oldtime =datetime.datetime.now()-datetime.timedelta(days=10)
# onefiletime >= oldtime