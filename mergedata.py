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
import copy													

class Merge(object) :
	def __init__(self) :
		self.save_this = "backup"

	def merge_xx_xx(self,sbmeter_last,sbmeter_cur) :
		sbmeter = {}
		for onedata in list(sbmeter_cur) :
			if sbmeter_last.get(onedata,None) is not None :
				gapvalue = round((float(sbmeter_cur[onedata]['reading']) - float(sbmeter_last[onedata]['reading'])),2)
				meterid = sbmeter_cur[onedata]['meterId']
				sbmeter[meterid] = gapvalue
		return sbmeter
		

