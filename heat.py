#!/usr/bin/env python
# encoding: utf-8

"""
Get data from http://heat.qq.com every five minitues , then record the result in .csv format
Covering (-9000,9000)*(-18000,18000) points
"""

import os
import sys
import json
import time
import logging
import urllib2
import signal
import datetime
import subprocess
import logging.config
from apscheduler.schedulers.background import BackgroundScheduler

# save all 18000*36000 points in a dict 
points = dict()
# Get process id
pid = os.getpid()
# init logger handler
logger = None

def graceful_exit(signum, frame):
    print 'Shutdown task immediately and exit ...'
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
			httpRequest(url,retry-1)
	except:
		httpRequest(url,retry-1)

def create(name,type=""):
	"""mkdir or touch file"""
	if type == "d":
		cmd = "mkdir -p {0}".format(name)
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

def job_function():
	""" 1. get origin data from the website
		2. format
		3. record processed data into .csv file
	"""
	
	timeNow = datetime.datetime.now().strftime("%F %T")
	base_url = "http://xingyun.map.qq.com/api/getPointsByTime_all_new.php?count=4&rank={rank}&time={time}"
	logger.info("Task begin ...")
	originData = ""
	timeSysNow = timeNow
	for i in range(4):
		url = base_url.format(rank=i,time=timeNow)
		retryTime = 3
		ret = json.loads(httpRequest(url,retryTime))
		if ret == "failed":
			logger.error("Http request {0} failed after {1} times").format(url, retryTime)
			os.kill(pid,signal.SIGINT)
		
		timeSysNow = ret["time"]
		originData += ret["locs"]

	if not os.path.exists("data"):
		create("data","d")

	with open("data/{0}.csv".format(timeSysNow),"w") as f:
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
	scheduler = BackgroundScheduler()
	scheduler.add_job(job_function, 'interval', minutes=5, max_instances=10)
	scheduler.start()

	try:
		while True:
			time.sleep(2)
	except (KeyboardInterrupt, SystemExit):
		scheduler.shutdown()