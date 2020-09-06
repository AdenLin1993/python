import requests
import re
import os
import configparser
from requests_ntlm import HttpNtlmAuth
import pandas as pd

class Fetch(object) :
    def __init__(self):
        if not os.path.exists('./temporaryfile'):
            os.makedirs('./temporaryfile')
        self.config = configparser.RawConfigParser()
        self.config.read('./config.cfg')
        self.user = self.config.get('EIP','user')
        self.password = self.config.get('EIP','password')
        self.auth = HttpNtlmAuth(self.user,self.password)

    def fetch_xxxx(self): 
        try :
            """ fa3a報表的路徑 """
            fa3a_dri = "https://mfgkm-wzs.wistron.com/P3A-War_room/DocLib1/WZS%20Plant3%20Final%20assembly%20Schedule-MZ3100(Building%203A)"
            resp_dri = requests.get(fa3a_dri,auth=self.auth,verify = False)
            dri_content = resp_dri.content.decode("utf-8")
            filepat = re.compile("id=/P3A-War_room/DocLib1/.*?\d\d\d\d\d\d[A,B,C].xlsx")
            filelist = filepat.findall(dri_content)
            DATELIST = []
            datepat = re.compile("\d\d\d\d\d\d")
            for onedate in filelist :
                date = datepat.findall(onedate)[0]
                DATELIST.append(date)
            LAST_DATE = max(DATELIST)

            for onedate in filelist :
                date = datepat.findall(onedate)[0]
                if date == LAST_DATE :
                    filename_content = onedate
            filenamepat = re.compile("P3A-War_room/DocLib1/.*?\d\d\d\d\d\d[A,B,C].xlsx")
            filename = filenamepat.findall(filename_content)[0]
            url = "https://mfgkm-wzs.wistron.com/{}".format(filename)
            resp = requests.get(url,auth=self.auth,verify=False)
            filecontent = resp.content
            with open ('./temporaryfile/xxxx.xlsx','wb') as f:
                    f.write(filecontent)
        except Exception as inst :
            print("fetch_fa_3a fail !")