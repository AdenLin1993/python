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

""" 创建一个从SFTP共享盘拉取数据的类"""
class Fetch(object):
	"""重写类的初始化属性"""
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
		
		"""建立与SFTP共享盘连接的客户端"""
		try :
			SFtp_transport = paramiko.Transport(self.config.get('WiGPSMOH_SFTP','sftphost'),int(self.config.get('WiGPSMOH_SFTP','sftpport')))
			SFtp_transport.connect(username = self.config.get('WiGPSMOH_SFTP','sftpuser'),password = self.config.get('WiGPSMOH_SFTP','sftppw'))
			self.SFtp_Conn = paramiko.SFTPClient.from_transport(SFtp_transport)
		except Exception as inst:
			print('Sftp Connection Fail')
			print(inst)
			logging.error('SFtp Connection Fail')
			logging.error(inst)
	
	"""这是一个从SFTP共享盘拉取数据的函数范例，remotedri是远程目录的路径，filetype是文件后缀属性，filestring是文件名称中含有哪些字符"""
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
					"""通过创建好的SFTP共享盘客服端和相关参数，下载指定的文件到程式运行的主机存储。Remotepath是要下载的远程文件路径，Localpath是指定的本地文件路径"""
					self.SFtp_Conn.get(Remotepath,Localpath)
					SuccessFiles+= 1
			except Exception as inst :
				print('Download {0} Fail'.format(onefile))
				print(inst)
		print("sftp download Total Number of Files: "+ str(TotalFiles) +"; Success Files: " +str(SuccessFiles))
		logging.info("sftp download Total Number of Files: "+ str(TotalFiles) +"; Success Files: " +str(SuccessFiles))
	
	"""这是一个从SFTP共享盘删除文件的函数范例，remotedri是远程目录的路径，successpush是要删除文件名称的一个list"""
	"""demoformat:delete_xxxx("/Spec/1LED00_OPR/AllPlant_QIS/",["NPI_PRINFO1.xls","NPI_PRINFO2.xls","NPI_PRINFO3.xls"])"""
	def delete_xxxx(self,remotedri,successpush) :
		print('Start delete {0}'.format(remotedri))
		logging.info('Start delete {0}'.format(remotedri))
		for onefile in successpush :
			try :
				Remotepath = remotedri+'{0}'.format(onefile)
				print(Remotepath)
				"""通过创建好的SFTP客户端和指定的特征，Remotepath是要删除文件的远程路径"""
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
