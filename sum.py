#coding: utf-8

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

def getDatetime(time, sign="start"):
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
        return "OK", newTime
    except:
        return "ERROR", "转换日期出错"

def getFile(startTime, endTime, step):
    timeDelta = DT.timedelta(minutes=5)
    status, startTime = getDatetime(startTime)
    status, endTime = getDatetime(endTime, "end")

    count = 0
    mergeList = []
    tmpList = []
    while startTime <= endTime:
        if count == step * 60:
            count = 0
            mergeList.append(tmpList)
            tmpList = []
        elif startTime == endTime:
            tmpList.append(startTime.strftime("%F_%T.csv"))
            mergeList.append(tmpList)
            break
        else:
            tmpList.append(startTime.strftime("%F_%T.csv"))
            count += 5
            startTime += timeDelta

    return mergeList

def outputFile(fileList):
    dirName = DT.datetime.now().strftime("%F %T")
    os.mkdir("/home/project/heatqq/Merge_"+dirName)
    
    os.chdir("/home/project/heatqq/Merge_"+dirName)
    for fl in fileList:
        fileName = "merge_"+fl[0]
        posDict = {}
        with open(fileName, "w") as f:
            f.write("lat,lng,qqheat\n")
            for name in fl:
                prefix = name.split('_')[0]
                if not os.path.exists("/home/project/heatqq/data/"+prefix+"/"+name):
                    print "/home/project/heatqq/data/"+prefix+"/"+name
                    continue
                fr = open("/home/project/heatqq/data/"+prefix+"/"+name, "r")
                line = fr.readline().strip()
                line = fr.readline().strip()
                while line:
                    key = line.split(',')[0]+"-"+line.split(',')[1]
                    num = line.split(',')[2]
                    s = posDict.get(key, 0)
                    posDict[key] = s + int(num)
                    line = fr.readline().strip()
                fr.close()
            for key, num in posDict.items():
                f.write(key.split('-')[0]+","+key.split('-')[1]+","+str(num)+"\n")

    os.chdir("..")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="根据步长计算开始时间至结束时间内的累加值")
    parser.add_argument("start_time", help="开始时间(2017-01-01 00:00:00)")
    parser.add_argument("end_time", help="结束时间(2017-01-01 12:00:00)")
    parser.add_argument("step", default=1, type=int, help="步长，最小（默认）1小时")

    args = parser.parse_args()
    #print args.start_time, args.end_time, args.step
    startTime = args.start_time
    endTime = args.end_time
    step = args.step
    ret = judgeLegal(startTime, endTime, step)
    if ret == "OK":
        print "输入校验通过~"
    else:
        print ret
        sys.exit(-1)

    fileList = getFile(startTime, endTime, step)
    outputFile(fileList)
