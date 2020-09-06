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
import paramiko

class Fetch(object):
	def __init__(self) :
		self.config = configparser.RawConfigParser()
		self.config.read('./config.cfg')
		if not os.path.exists('./Log'):
			os.makedirs('./Log')
		if not os.path.exists('./temporaryfile') :
			os.makedirs('./temporaryfile')
		logging.basicConfig(filename='./Log/'+datetime.datetime.today().strftime("%Y%m%d")+'.log'
			, level=logging.INFO
			, format='%(asctime)s %(message)s'
			, datefmt='%Y/%m/%d %I:%M:%S %p')
		try :
			SFtp_transport = paramiko.Transport(self.config.get('WiGPSMOH_SFTP','sftphost'),int(self.config.get('WiGPSMOH_SFTP','sftpport')))
			SFtp_transport.connect(username = self.config.get('WiGPSMOH_SFTP','sftpuser'),password = self.config.get('WiGPSMOH_SFTP','sftppw'))
			self.SFtp_Conn = paramiko.SFTPClient.from_transport(SFtp_transport)
		except Exception as inst:
			print('Sftp Connection Fail')
			print(inst)
			logging.error('SFtp Connection Fail')
			logging.error(inst)

	"""demoformat:fetch_xxxx("/Spec/1LED00_OPR/AllPlant_QIS/","xls",filestring = "NPI_PRINFO")"""	
	def fetch_xxxx(self,remotedri,filetpye,filestring = "."):
		print('Start Download {0}'.format(remotedri))
		logging.info('Start Download {0}'.format(remotedri))

		Files = self.SFtp_Conn.listdir(remotedri)
		TotalFiles = 0
		SuccessFiles = 0
		for onefile in Files :
			try :
				filecompile = re.compile('{}'.format(filestring))
				findlength = len(filecompile.findall(onefile))
				if onefile.lower().endswith('.{}'.format(filetpye)) and findlength > 0:
					TotalFiles+= 1
					Remotepath = remotedri+'{0}'.format(onefile)
					Localpath = './temporaryfile/{0}'.format(onefile)
					self.SFtp_Conn.get(Remotepath,Localpath)
					SuccessFiles+= 1
			except Exception as inst :
				print('Download {0} Fail'.format(onefile))
				print(inst)
		print("sftp download Total Number of Files: "+ str(TotalFiles) +"; Success Files: " +str(SuccessFiles))
		logging.info("sftp download Total Number of Files: "+ str(TotalFiles) +"; Success Files: " +str(SuccessFiles))

	"""demoformat:delete_xxxx("/Spec/1LED00_OPR/AllPlant_QIS/",["NPI_PRINFO1.xls","NPI_PRINFO2.xls","NPI_PRINFO3.xls"])"""
	def delete_xxxx(self,remotedri,successpush) :
		print('Start delete {0}'.format(remotedri))
		logging.info('Start delete {0}'.format(remotedri))
		for onefile in successpush :
			try :
				Remotepath = remotedri+'{0}'.format(onefile)
				print(Remotepath)
				self.SFtp_Conn.remove(Remotepath)
			except Exception as inst :
				print('delete {0} Fail'.format(onefile))
				print(inst)
		print('SFtp file delete Finish')
		logging.info('SFtp file delete Finish')

"""根據文檔日期進行篩選下載，將邏輯加在第48行"""
# onefiletime =datetime.datetime.strptime(onefile.split('_')[2][:8],'%Y%m%d')
# oldtime =datetime.datetime.now()-datetime.timedelta(days=10)
# onefiletime >= oldtime