#!/usr/bin/env python
# encoding: utf-8

import os
import mail
import shutil
import datetime
from util.const import receiveList

def judgeCompress(time):
    """
        check whether to compress the archive
    """
    yesterday = (time - datetime.timedelta(days=1)).strftime("%F")
    dirName = "data/{0}".format(yesterday)
    if os.path.exists(dirName):
        shutil.make_archive(dirName,"zip",dirName)
        shutil.rmtree(dirName)
        for rec in receiveList:
            mail.send(rec, dirName+"数据压缩完成", "昨日数据{0}已压缩完毕,请确认!".format(dirName))

if __name__ == "__main__":
    time = datetime.datetime.now()
    judgeCompress(time)
