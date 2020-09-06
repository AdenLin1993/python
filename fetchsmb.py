#!/usr//bin/python
#-*- coding: utf-8 -*-

import requests
import re
import os
import configparser
import logging
import time
import datetime
from smbclient import(listdir,mkdir,register_session,rmdir,scandir,link,open_file,remove,stat,symlink,)

class Fetch(object) :
	def __init__(self):
		self.config = configparser.RawConfigParser()
		self.config.read('./config.cfg')
		if not os.path.exists('./Log'):
			os.makedirs('./Log')
		if not os.path.exists('./temporaryfile'):
			os.makedirs('./temporaryfile')
		self.smbhost=self.config.get('P3DLA_SMB','smbhost')
		self.smbuser=self.config.get('P3DLA_SMB','smbuser')
		self.smbpw=self.config.get('P3DLA_SMB','smbpw')

		try :
			register_session(self.smbhost,self.smbuser,self.smbpw)
		except Exception as inst:
			print('SMB Connection Fail')
			print(inst)
			logging.error('SMB Connection Fail')
			logging.error(inst)

		logging.basicConfig(filename='./Log/'+datetime.datetime.today().strftime("%Y%m%d")+'.log'
			, level=logging.INFO
			, format='%(asctime)s %(message)s'
			, datefmt='%Y/%m/%d %I:%M:%S %p')

	"""demoformat:fetch_xxxx("10.41.52.124/mm/Scrap/output","xlsx","Scrap")"""
	def fetch_xxxx(self,remotedri,filetpye,filestring = "."):
		print('Start Download smbfile')
		logging.info('Start Download smbfile')

		Files = listdir(remotedri)
		TotalFiles = 0
		SuccessFiles = 0
		for onefile in Files :
			try : 
				filecompile = re.compile('{}'.format(filestring))
				findlength = len(filecompile.findall(onefile))  
				if onefile.lower().endswith('.{}'.format(filetpye)) and findlength > 0:
					Remotepath = os.path.join(remotedri,onefile)
					Localpath = './temporaryfile/{}'.format(onefile)
					TotalFiles+= 1
					with open_file(Remotepath,mode = "rb") as fr:
						file_bytes = fr.read()
					with open(Localpath,"wb") as fw :
						fw.write(file_bytes)
					SuccessFiles+= 1
			except Exception as inst :
				print('Download file Fail')
				print(inst)
		print("smb download Total Number of Files: "+ str(TotalFiles) +"; Success Files: " +str(SuccessFiles))
		logging.info("smb download Total Number of Files: "+ str(TotalFiles) +"; Success Files: " +str(SuccessFiles))		

"""根據文檔日期進行篩選下載，將邏輯加在第50行"""
# onefiletime =datetime.datetime.strptime(onefile.split('_')[2][:8],'%Y%m%d')
# oldtime =datetime.datetime.now()-datetime.timedelta(days=10)
# onefiletime >= oldtime