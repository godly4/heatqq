#!/usr/bin/env python
# encoding: utf-8

"""
Get data from http://heat.qq.com every five minitues , then record the result in .csv format
Covering (-9000,9000)*(-18000,18000) points
"""

import os
import json
import shutil
import ctypes
import logging
import urllib2
import signal
import datetime
import platform
import subprocess
import logging.config
import mail
import time as atime
from util.const import receiveList
from apscheduler.schedulers.background import BackgroundScheduler

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

# Get process id
pid = os.getpid()
# init logger handler
logger = None

def graceful_exit(signum, frame):
    print 'Shutdown task immediately and exit ...'
    for rec in receiveList:
        mail.send(rec, "HeatQQ program crashed", "The program crashed, please confirm!") 
    sys.exit(0)

# Catch `ctrl+C` or `kill -HUP` signal
signal.signal(signal.SIGINT, graceful_exit)

def httpRequest(url,retry=1):
	"""process the http request in retry times"""
	if retry==0:
		return "failed"
	try:
		req = urllib2.Request(url)
		res = urllib2.urlopen(req)
		if res.getcode() == 200:
			return res.read()
		else:
			return httpRequest(url,retry-1)
	except:
		return httpRequest(url,retry-1)

def create(name,type=""):
	"""mkdir or touch file"""
	if type == "d":
		cmd = "mkdir \"{0}\"".format(name)
		ret = subprocess.call(cmd,shell=True)
		if ret != 0:
			print "Create directory {0} failed!".format(name) , cmd
			os.kill(pid,signal.SIGINT)
	else:
		dirName = os.path.dirname(os.path.realpath(name))
		if not os.path.exists(dirName):
			create(dirName,'d')
		cmd = "touch {0}".format(name)
		ret = subprocess.call(cmd,shell=True)
		if ret != 0:
			print "Create file {0} failed!".format(name) , cmd
			os.kill(pid,signal.SIGINT)

def init():
	"""Init the dict and essential files"""
	if not os.path.exists("heat.pid"):
		create("heat.pid")
		
	with open("heat.pid","w") as f:
		f.write(str(pid))

	if not os.path.exists("logs/heat.log"):
		create("logs/heat.log")

	# create logger
	global logger
	logging.config.fileConfig('logging.conf')
	logger = logging.getLogger('heat')

def judgeCompress(time):
    """
        check whether to compress the archive
    """
    yesterday = (time - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    dirName = "data/{0}".format(yesterday)
    if os.path.exists(dirName):
        shutil.make_archive(dirName,"zip",dirName)
        shutil.rmtree(dirName)
        for rec in receiveList:
            mail.send(rec, dirName+" has been compressed", "Last day's({0}) data has been compressed, please confirm!".format(dirName))

def judgeDisk(diskName):
    """
        check whether the disk is almost full!
    """
    ret = 0
    if platform.system() == 'Windows':
        free_bytes = ctypes.c_ulonglong(0)
        ctypes.windll.kernel32.GetDiskFreeSpaceExW(ctypes.c_wchar_p(diskName), None, None, ctypes.pointer(free_bytes))
        ret = (free_bytes.value/1024/1024/1024)
    else:
        st = os.statvfs(diskName)
        ret = (st.f_bavail * st.f_frsize/1024/1024)

    if ret<= 1:
        for rec in receiveList:
            mail.send(rec, "Disk alert!","Remain less than 1 GB, please confirm!") 
        os.kill(pid,signal.SIGINT)

def job_function():
    """ 1. get origin data from the website
        2. format
        3. record processed data into .csv file
    """
    time = datetime.datetime.now()
    #判断是否启用压缩
    judgeCompress(time)
    #判断磁盘是否存在报警
    judgeDisk("C:\\")
    dateNow = time.strftime("%Y-%m-%d")
    timeNow = time.strftime("%H:%M:%S")
    base_url = "http://xingyun.map.qq.com/api/getPointsByTime_all_new.php?count=4&rank={rank}&time={time}"
    logger.info("Task begin ...")
    originData = ""
    timeSysNow = dateNow+" "+timeNow
    for i in range(4):
        url = base_url.format(rank=i,time=timeSysNow)
        retryTime = 3
        result = httpRequest(url,retryTime)
        if result == "failed":
            for rec in receiveList:
                mail.send(rec, "HttpRequest error!","Request {0} is failed!Please check!".format(url))
            os.kill(pid,signal.SIGINT)
        ret = json.loads(result)
        if ret == "failed":
            logger.error("Http request {0} failed after {1} times").format(url, retryTime)
            os.kill(pid,signal.SIGINT)

        timeSysNow = ret["time"]
        originData += ret["locs"]

    currentDir = "data/{0}".format(dateNow)
    if not os.path.exists(currentDir):
        create(currentDir,"d")

    timeSysNow = timeSysNow.replace(" ","_").replace(":","-")
    with open(currentDir+"/{0}.csv".format(timeSysNow),"w") as f:
        f.write("lat,lng,qqheat\n")
        count = 0
        dataList = originData.split(',')
        while count <= len(dataList)-3:
            f.write("{0},{1},{2}\n".format(int(dataList[count])/100.0,int(dataList[count+1])/100.0,dataList[count+2]))
            count += 3

    logger.info("Task end, get all data at {0}".format(timeSysNow))

if __name__ == "__main__":
    init()
    print('Press `Ctrl+{0}` or run `kill -HUP` to exit gracefully'.format('Break' if os.name == 'nt' else 'C'))
    #job_function()
    scheduler = BackgroundScheduler()
    scheduler.add_job(job_function, 'interval', minutes=5, max_instances=10)
    scheduler.start()

    try:
        while True:
            atime.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
