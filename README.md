fetches.py  
这个python脚本中写好了一个如何从ES数据库拉取数据的类范例。实现这个功能的指令如下：

主要理解这句指令的上下文：page = self.EsClient.search(index=self.Index, size=100000, body=query_body, scroll='50m')

其中使用了与ES数据库通信的包elasticsearch；使用到的相关方法或需获取其他新方法可以参考 https://elasticsearch-py.readthedocs.io/en/master/



fetchexcel.py
这个python脚本写好一个如何从本地拉取一个指定特征的excel工作簿，实现这个功能的指令如下：

主要理解这句指令的上下文：df = pd.read_excel(filepath,targetsheet[0],header = 0)

其他都是个人以此条指令的再封装，为了自己方便使用，和应对一些特殊情况下也能正常使用的需求，使用的相关方法参考 https://pandas.pydata.org/pandas-docs/stable/reference/index.html 。



fetchftp.py
这个python脚本写好一个如何从FTP共享盘下载指定特征的文件，实现这个功能的指令如下：

主要理解这句指令的上下文：self.FtpClient.retrbinary("RETR "+onefile,open(TmpPath,'wb').write)

其他指令是应对需求做的再封装，为了方便自己使用，和应对一些特殊情况下也能正常使用的需求，其中使用的相关方法参考 https://docs.python.org/3/library/ftplib.html



fetchkafka.py
这个python脚本写好一个如何从kafka数据库拉取数据的方法，实现这个功能的指令如下：

主要理解这句指令的上下文：
msg = self.consumer.poll(1)
if msg is None:
  continue
if msg.error():
  print('Consumer error: {}'.format(msg.error()))
  continue
data = json_util.loads(msg.value())
相关使用的方法参考：https://github.com/confluentinc/confluent-kafka-python



fetchmariadb.py
这个python脚本写好一个如何从mariadb数据库拉取数据的方法，实现这个功能的指令如下：
with self.MySqlConn.cursor() as cursor:
  sql = """SELECT * 
  FROM dat_energy.wzs_sb_base_data 
  WHERE wzs_sb_base_data.batchid = 
  (SELECT MAX(wzs_sb_base_data.batchid) 
  FROM dat_energy.wzs_sb_base_data) 
  ;"""
  cursor.execute(sql)
  meterbase = cursor.fetchall()
  
sqlEngine = create_engine("mysql+pymysql://{0}:{1}@{2}:3306/{3}?charset=utf8".format(self.user,self.pwd,self.host,self.db))
self.dbConnection = sqlEngine.connect()
相关使用方法参考文档：
https://github.com/PyMySQL/PyMySQL/
https://docs.sqlalchemy.org/en/13/dialects/mysql.html



fetchsftp.py
这个python脚本写好一个如何从SFTP共享盘下载指定特征的文件，实现这个功能的指令如下：

主要理解这句指令的上下文：self.SFtp_Conn.get(Remotepath,Localpath)
self.SFtp_Conn.remove(Remotepath)

其他指令是应对需求做的再封装，为了方便自己使用，和应对一些特殊情况下也能正常使用的需求，其中使用的相关方法参考 
http://docs.paramiko.org/en/stable/api/sftp.html



fetchsmb.py
这个python脚本写好一个如何从SMB共享盘下载指定特征的文件，实现这个功能的指令如下：

主要理解这句指令的上下文：					
with open_file(Remotepath,mode = "rb") as fr:
						file_bytes = fr.read()
with open(Localpath,"wb") as fw :
						fw.write(file_bytes)
其他指令是应对需求做的再封装，为了方便自己使用，和应对一些特殊情况下也能正常使用的需求，其中使用的相关方法参考 
https://github.com/jborean93/smbprotocol


fetweb.py
这个python脚本写好爬取某个网站webdav资源的方法，通过url拿取资源。
网络上有写好专门从webdav拿取资源的包，之前寻找测试没有找到能用的包。需要继续测试其他的包。
网络上爬取资源的资料均可参考。



pushes.py
这个python 脚本写好往es数据库推数据的方法。
主要理解这个指令的上下文：

helpers.bulk(client = self.EsClient,actions = actions)

相关方法参考   https://elasticsearch-py.readthedocs.io/en/master/



pushmariadb.py
这个python脚本写好往mariadb数据库推数据的方法。
主要理解这个指令的上下文：

Cur = self.MySqlConn.cursor()

Cur.execute(SqlBuffer)

self.MySqlConn.commit()

Cur.close()

相关方法参考
https://github.com/PyMySQL/PyMySQL/
https://docs.sqlalchemy.org/en/13/dialects/mysql.html



pushmqtt.py
这个python脚本写好往mqtt数据库推数据的方法。
主要理解这个指令的上下文：

publish.multiple(msgs,hostname="zsarm-mqtt-p03.wzs.wistron",port=9001,client_id="carey_lin", keepalive=60,transport="websockets")

相关方法参考
https://github.com/eclipse/paho.mqtt.python/blob/master/examples/publish_multiple.py
