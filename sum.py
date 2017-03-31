#coding: utf-8
from __future__ import print_function

import re
import os
import sys
import argparse
import datetime as DT

def judgeLegal(startTime, endTime, step):
    if step < 1:
        return "最小时间间隔1小时!"
    else:
        status, startTime = getDatetime(startTime)
        if status == "ERROR":
            return startTime
        status, endTime = getDatetime(endTime)
        if status == "ERROR":
            return endTime
        if startTime >= endTime:
            return "起始时间应小于结束时间!"

    return "OK"

def getDatetime(time, sign="start", init=False):
    timeList = re.findall("(\d\d\d\d)-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d)", time)
    try:
        year  = int(timeList[0][0])
        month = int(timeList[0][1])
        day   = int(timeList[0][2])
        hour  = int(timeList[0][3])
        minu  = int(timeList[0][4])
        if sign == "start":
            minu = minu - minu % 5
        else:
            if minu % 5 != 0:
                minu = minu + 5 - minu % 5
        sec  = int(timeList[0][5])

        newTime = DT.datetime(year, month, day, hour, minu, sec)
        if init:
            if sign == "start":
                newTime = DT.datetime(year, month, day, 0, 0, 0)
            else:
                newTime = DT.datetime(year, month, day, 23, 55, 0)
        return "OK", newTime
    except:
        return "ERROR", "转换日期出错"

def getFile(startTime, endTime, step, exclude):
    timeDelta = DT.timedelta(minutes=5)
    status, oStart = getDatetime(startTime)
    status, oEnd = getDatetime(endTime, "end")

    def getList(start, end, equal=True):
        count = 0
        mergeList = []
        tmpList = []
        while start <= end:
            if count == step * 60:
                count = 0
                mergeList.append(tmpList)
                tmpList = []
            elif start == end:
                if equal:
                    tmpList.append(start.strftime("%F_%T.csv"))
                mergeList.append(tmpList)
                break
            else:
                tmpList.append(start.strftime("%F_%T.csv"))
                count += 5
                start += timeDelta

        return mergeList

    if not exclude:
        return getList(oStart, oEnd)
    else:
        status, eStart = getDatetime(startTime, init=True)
        status, eEnd   = getDatetime(endTime, "end", init=True)
        mergeList = getList(eStart, oStart, False)
        mergeList.extend(getList(oEnd+timeDelta, eEnd))
        if step == 24:
            retList = mergeList[0]
            retList.extend(mergeList[1])
            mergeList = [retList]

        return mergeList

def outputFile(fileList):
    dirName = DT.datetime.now().strftime("%F %T")
    os.mkdir("Merge_"+dirName)
    
    os.chdir("Merge_"+dirName)
    for fl in fileList:
        fileName = fl[0]
        posDict = {}
        with open(fileName, "w") as f:
            f.write("lat,lng,qqheat\n")
            for name in fl:
                prefix = name.split('_')[0]
                if not os.path.exists("../data/"+prefix+"/"+name):
                    #print("/Users/godly/Desktop/HeatQQ/data/"+prefix+"/"+name)
                    continue
                print("处理"+name+"文件中...")
                fr = open("../data/"+prefix+"/"+name, "r")
                line = fr.readline().strip()
                line = fr.readline().strip()
                while line:
                    key = line.split(',')[0]+"@"+line.split(',')[1]
                    num = line.split(',')[2]
                    s = posDict.get(key, 0)
                    if not num:
                        num = 0
                    posDict[key] = s + int(num)
                    line = fr.readline().strip()
                fr.close()
            for key, num in posDict.items():
                f.write(key.split('@')[0]+","+key.split('@')[1]+","+str(num)+"\n")

    os.chdir("..")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="根据步长计算开始时间至结束时间内的累加值\n"\
                    +"如要计算黑夜时间请加-e参数并设置step值为24,常用形式如下：\n"
                    +"1、计算当天汇总:       python sum.py \"2017-03-31 00:00:00\" \"2017-03-31 23:55:00\" 24\n"
                    +"2、计算当天每小时汇总: python sum.py \"2017-03-31 00:00:00\" \"2017-03-31 23:55:00\" 1\n"
                    +"3、计算当天白昼汇总:   python sum.py \"2017-03-31 07:00:00\" \"2017-03-31 18:55:00\" 12\n"
                    +"4、计算当天黑夜汇总:   python sum.py \"2017-03-31 07:00:00\" \"2017-03-31 18:55:00\" 24 -e\n",formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("start_time", help="开始时间(2017-01-01 00:00:00)")
    parser.add_argument("end_time", help="结束时间(2017-01-01 12:00:00)")
    parser.add_argument("step", default=1, type=int, help="步长，最小（默认）1小时")
    parser.add_argument("-e", action="store_true", default=False, help="排除当前区间")

    args = parser.parse_args()
    #print args.start_time, args.end_time, args.step
    startTime = args.start_time
    endTime = args.end_time
    step = args.step
    ret = judgeLegal(startTime, endTime, step)
    if ret == "OK":
        print("输入校验通过~")
    else:
        print(ret)
        sys.exit(-1)

    fileList = getFile(startTime, endTime, step, args.e)
    #print(fileList)
    outputFile(fileList)
