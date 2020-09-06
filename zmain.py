#!/usr/bin/python
#-*- coding: utf-8 -*-

import fetchexcle
import pushmariadb
import mergedata
import time
import datetime

if __name__ == '__main__' :
	"""create a class that fun is fetching all type data from excel"""
	fetch_excel = fetchexcle.Fetch()

	"""create a class that fun is to join and push data"""
	push_mariadb = pushmariadb.Push()

	filepath = fetch_excel.findpath('./temporaryfile','xlsx',filestring = '.')
	df_sb = fetch_excel.getdataframe(filepath,'IT')
