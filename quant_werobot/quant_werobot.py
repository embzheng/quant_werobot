#coding:utf-8
# Filename:hello_world.py
# 验证服务器，并且收到的所有消息都回复'Hello World!'
 
import werobot
import os
import numpy as np
import pandas as pd
import sqlalchemy 
from datetime import datetime,timedelta
import logging
import logging.config
import sys

# 设置进程名
import setproctitle
proc_title = "werobot_main"
setproctitle.setproctitle(proc_title)

DEBUG = False
log = logging.getLogger('werobot')
PRJ_IDR = '/home/embzheng'

class StdRedirection:    
    def __init__(self, log):
        self.__console__=sys.stdout
        self.__stderr__=sys.stderr
        self.logger = log
        
    def write(self, output_stream):
        self.logger.info(output_stream.split())
        
    def reset(self):
        sys.stdout=self.__console__
        sys.stderr=self.__stderr__
        
def log_setup(log):
    log_dir = 'logfile'
    curpath = PRJ_IDR
    log_dir_path = os.path.join(curpath, log_dir)
    log_file_name = os.path.join(log_dir_path, 'werobot_main.log')
    if not os.path.isdir(log_dir_path):  # 无文件夹时创建
        os.makedirs(log_dir_path)
    
    format_rule = "[%(name)s]%(asctime)s-%(levelname)s[%(lineno)d]: %(message)s"   
    #2.设置log日志的标准输出打印，如果不需要在终端输出结果可忽略        
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    #formatter = logging.Formatter("%(asctime)s--%(message)s" )
    formatter = logging.Formatter(format_rule)
    console.setFormatter(formatter)
    log.addHandler(console)

    enable_file_log = True
    if enable_file_log:
        log.setLevel(logging.INFO)  
        formatter = logging.Formatter(format_rule)        
        filehandler = logging.handlers. RotatingFileHandler(log_file_name, mode='a', maxBytes=1024000, backupCount=10)#每 1024Bytes重写一个文件,保留2(backupCount) 个旧文件
        filehandler.setFormatter(formatter)
        log.addHandler(filehandler) 
    
def sql_engine(my_path,db_name):
    sql_path='sqlite:///'
    if not os.path.exists(my_path):
        os.makedirs(my_path)
    file_path=os.path.join(my_path,db_name)
    file_path=os.path.abspath(file_path)
    db_path=sql_path+file_path
    engine = sqlalchemy.create_engine(db_path)
    return engine
    
class SqlData(object):
    def __init__(self,my_path=os.getcwd(),db_name='quant_server.db'):
        self.engine=sql_engine(my_path,db_name)
        
    #获取指数指标数据
    def get_bigsmall_data(self, trade_date=''):
        if trade_date != '':
            sql=f"select * from bigsmall where trade_date='{trade_date}'"
        else:
            sql=f"select * from bigsmall order by trade_date desc limit 0,10"
        log.debug(sql)
        df=pd.read_sql(sql, self.engine)        
        df.index=pd.to_datetime(df.trade_date) 
        df=(df.sort_index())
        return df
            
    def get_north_data(self, trade_date=''):
        if trade_date != '':
            sql=f"select * from north where trade_date='{trade_date}'"
        else:
            sql=f"select * from north order by trade_date desc limit 0,10"
        log.debug(sql)
        df=pd.read_sql(sql, self.engine)      
        df=(df.sort_values(by='trade_date'))  
        df.index=pd.to_datetime(df.trade_date) 
        return df  
    
    def get_qushi_data(self, name, trade_date=''):
        if trade_date != '':
            sql=f"select * from qushi where trade_date='{trade_date}' and name='{name}'"
        else:
            sql=f"select * from qushi where name='{name}' order by trade_date desc limit 0,10"
        log.debug(sql)
        df=pd.read_sql(sql, self.engine) 
        df=(df.sort_values(by='trade_date'))         
        df.index=pd.to_datetime(df.trade_date) 
        return df 
        
# sql 操作实例        
sql = SqlData(my_path=PRJ_IDR)       
robot = werobot.WeRoBot(token='embzheng')        

# @robot.handler 处理所有消息
# @robot.handler
# def hello(message):
#     return 'Hello World!'

# @robot.text 修饰的 Handler 只处理文本消息
@robot.text
def echo(message):
    log.info(f"收到文本消息:{message.content}")
    if message.content.find('全') != -1:
        zs = zeshi()
        xh = bigsmall()
        return zs + xh
    if message.content.find('信号') != -1:
        return bigsmall()
    elif message.content.find('择时') != -1:
        return zeshi()
    else:        
        return message.content
    
def bigsmall():    
    df = sql.get_bigsmall_data()    
    if len(df) == 0:
        log.error("获取sql数据失败")
        return
    trade_date = df.values[-1][0]
    log.debug(df.values)
    df = sql.get_bigsmall_data(trade_date=trade_date) 
    text = "交易日：%s\n" %(trade_date)    
    for idx,data in df.iterrows():
        #print("[{}]: {}".format(idx,data))    
        big = data[1]
        small = data[2]
        style = data[3]
        diff = data[4]  
        correl = data[5]  
        text += f'标的:[{big},{small}]\n强者:{style}\n差值:%2.2f%%\n相关性:%2.2f\n\n' %(diff,correl)
    log.info(text)
    return text   

def zeshi_north():
    df = sql.get_north_data()    
    if len(df) == 0:
        log.error("获取sql数据失败")
        return
    trade_date = df.values[-1][0]
    log.debug(df.values)
    text = "交易日：%s\n" %(trade_date)    
    if df.values[-1][1] == 1:
        signal_str = '看涨'
    elif df.values[-1][1] == -1:
        signal_str = '看跌'
    else:
        signal_str = '观望'
    text += (f'北向择时:{signal_str}\n净流入:%2.2f亿\n临界值:[%2.2f,  %2.2f]\n中位数:%2.2f亿\n\n' 
        %(df.values[-1][2]/10000, df.values[-1][3]/10000, df.values[-1][4]/10000, df.values[-1][5]/10000))
    log.info(text)    
    return text 

def zeshi_qushi(name='上证综指'):
    df = sql.get_qushi_data(name=name)    
    if len(df) == 0:
        log.error("获取sql数据失败")
        return
    log.debug(df.values)
    trade_date = df.values[-1][0]
    text = "交易日：%s\n" %(trade_date)    
    text += f'{name}-趋势择时:\n短线[%d,  %d]\n长线[%d,  %d]\n\n' %(df.values[-1][3], df.values[-1][4], df.values[-1][5], df.values[-1][6])
    log.info(text)    
    return text 

def zeshi():    
    text = zeshi_qushi(name='上证综指')
    text += zeshi_qushi(name='沪深300')
    text += zeshi_north()    
    return text
 


     
def main():    
    log_setup(log)
    log.setLevel(logging.DEBUG)
    log.debug("开启调试log")
    # log重定向
    sys.stdout = StdRedirection(log)
    sys.stderr = StdRedirection(log)    
        
    robot.config["APP_ID"] = "xxx"
    robot.config["APP_SECRET"] = "xxx"
    client = robot.client
    if DEBUG:
        bigsmall()
        log.info("debug mode,return") 
        return
    # 需要认证的公众号才支持
    # client.create_menu({
    #     "button":[{
    #          "type": "click",
    #          "name": "轮动指标",
    #          "key": "bigsmall"
    #     }]
    # })
    log.info("开始监听微信服务器")
    # 让服务器监听在 0.0.0.0:80
    robot.config['HOST'] = '0.0.0.0'
    robot.config['PORT'] = 80
    
    try:
        robot.run()
    # except SocketError as e:
    #     if e.errno != errno.ECONNRESET:
    #         raise
    # pass
    except Exception as r:
        s = sys.exc_info()
        log.error("Error '%s' happened on line %d" % (s[1],s[2].tb_lineno))  

main()
