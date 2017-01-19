#! /user/bin/env python
#encoding=utf8

import requests
import urllib
import logging
import time
import gzip
from datetime import datetime


url_fmt  = "http://xingyun.map.qq.com/api/getPointsByTime_all_new.php?count=4&rank={0}&time={1}"
time_fmt = "%Y-%m-%d %H:%M:00"
fmt = '[%(asctime)s-%(levelname)s]:%(message)s'

logger = logging.getLogger("heat_qq")
file_handler = logging.FileHandler("heat_qq.log")
console_handler = logging.StreamHandler()
formatter = logging.Formatter(fmt)
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)

s = requests.session()
s.headers["User-Agent"] = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.82 Safari/537.36'

def get_current_time():
    return urllib.quote(datetime.now().strftime(time_fmt), safe=":")

def get_json(url, retry=10):
    for i in range(retry):
        resp = s.get(url)
        if resp.ok:
            return resp.json()
    logger.warning("%s retry too many times" % url)

def get_data():
    time_str = get_current_time()
    locs_str = u''
    time_rec = u''
    for i in range(4):
        url = url_fmt.format(i, time_str)
        print url
        result_json = get_json(url)
        locs_str += result_json['locs']
        time_rec = result_json['time']
        print len(locs_str), time_rec
    n = locs_str.split(",")
    result_list = []
    for i in range(len(n)/3):
        result_list.append(','.join([str(int(n[3*i+0])/100.0), str(int(n[3*i+1])/100.0), n[3*i+2]]))
    return {
        "locs": result_list,
        "time": time_rec,
    }

if __name__ == '__main__':
    while True:
        try:
            data = get_data()
            with gzip.open("%s.zip" % time.strftime("%Y-%m-%d %H%M%S"), "w") as f:
                f.write('lat,lng,qqheat\n' + '\n'.join(data['locs']))
            time.sleep(300)
        except Exception as e:
            logger.exception(str(e))
