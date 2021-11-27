#!/usr/bin/python
import sys
import requests
import json
import os
import re
import numpy as np
from operator import itemgetter
import matplotlib.pyplot as plt
proxies={
    'http':None
}

def queryInfo(server,file,index,sqlname,filePath):
    sql = open(filePath).read().replace(';', '').replace('\r\n', '\n').strip()
    headers={'X-Presto-User':'root'}
    response=requests.get(server + '/v1/query',headers=headers,proxies=proxies)
    content=response.text
    queryId=sorted(filter(lambda  elem:elem['query'].replace('\r\n','\n').strip()==sql.replace(';','').replace('\r\n','\n').strip(),json.loads(content)), key=itemgetter('queryId'),reverse=True)[0]['queryId']
    content=requests.get(server + '/v1/query/' + queryId + '?pretty', headers=headers, proxies=proxies).text
    queryJson=json.loads(content)
    jsonDir=os.path.dirname(file)+'/json/'
    mkdirs(jsonDir)
    jsonFile=jsonDir+str(index)+'.json'
    with open(jsonFile, mode='w') as filename:
        filename.write(content)

    for operatorSummary in queryJson['queryStats']['operatorSummaries']:
        operatorType=operatorSummary['operatorType']
        stageId=operatorSummary['stageId']
        pipelineId=operatorSummary['pipelineId']
        addInputWall=operatorSummary['addInputWall']
        addInputCpu=operatorSummary['addInputCpu']
        getOutputWall=operatorSummary['getOutputWall']
        getOutputCpu=operatorSummary['getOutputCpu']
        finishWall=operatorSummary['finishWall']
        finishCpu=operatorSummary['finishCpu']
        inputPositions = operatorSummary['inputPositions']
        inputDataSize = operatorSummary['inputDataSize']
        outputPositions = operatorSummary['outputPositions']
        outputDataSize = operatorSummary['outputDataSize']
        with open(file, mode='a') as filename:
            filename.write(sqlname+'\t'+str(index)+'\t'+operatorType+'\t'+str(stageId)+'\t'+str(pipelineId)+'\t'+addInputWall+'\t'+addInputCpu+'\t'+getOutputWall+'\t'+getOutputCpu+'\t'+finishWall+'\t'+finishCpu+'\t'+str(inputPositions)+'\t'+str(inputDataSize)+'\t'+str(outputPositions)+'\t'+str(outputDataSize)+'\n')

def transtimeunit(timeWithUnit):#trans to ns
    timestr=''.join(re.findall('\d*\.\d+',timeWithUnit))
    unit=''.join(re.findall(r'[A-Za-z]',timeWithUnit))
    time=float(timestr)
    if unit=='us':
        time*=1000
    elif unit=='ms':
        time*=1000000
    elif unit=='s':
        time*=1000000000
    elif unit=='ns':
        time*=1
    else:
        raise RuntimeError(unit+'unit not support')
    return time
def avgValue(filelocation,writefile):
    index = 0
    dictValue = {}
    for line in open(filelocation):
        index += 1
        if index == 1:
            continue
        arr = line.split('\t')
        key = arr[0] + ',' + arr[3] + ',' + arr[4] + ',' + arr[2]
        value = dictValue.get(key)
        if value is None:
            value = [arr]
        else:
            value.append(arr)
        dictValue[key] = value
    for key in dictValue.keys():
        addInputWall = 0.0;
        addInputCpu = 0.0;
        getOutputWall = 0;
        getOutputCpu = 0;
        finishWall = 0;
        finishCpu = 0;
        operatorValue = dictValue[key]
        for i in range(len(operatorValue)):
            if i==0:
                continue
            item = operatorValue[i]
            addInputWall += transtimeunit(item[5])
            addInputCpu += transtimeunit(item[6])
            getOutputWall += transtimeunit(item[7])
            getOutputCpu += transtimeunit(item[8])
            finishWall += transtimeunit(item[9])
            finishCpu += transtimeunit(item[10])
            i = i + 1
        if not os.path.exists(writefile):
            with open(writefile,'w') as f:
                f.write('key\tavg_addInputWall\tavg_addInputCpu\tavg_getOutputWall\tgetOutputCpu\tfinishWall\tfinishCpu\n')
        with open(writefile,'a+') as f:
            f.write(key+'\t'+str(addInputWall/len(operatorValue))+'\t'+str(addInputCpu/len(operatorValue))+'\t'+str(getOutputWall/len(operatorValue))+'\t'+str(getOutputCpu/len(operatorValue))+'\t'+str(finishWall/len(operatorValue))+'\t'+str(finishCpu/len(operatorValue))+'\n')

def mkdirs(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)

def comparefile(omnifile, olkfile):
    omniresultdir=os.path.dirname(omnifile)+'/charter'
    olkresultdir=os.path.dirname(olkfile)+'/charter'
    mkdirs(omniresultdir)
    mkdirs(olkresultdir)

    omnifileDic=dict(sorted(ReadFileIntoDict(omnifile).items(),key=lambda kv:float(kv[1][1])+float(kv[1][3])+float(kv[1][5]),reverse=True))
    olkfileDic=ReadFileIntoDict(olkfile)

    index=1
    index = drarChart(index, olkfileDic, olkresultdir, omnifileDic, omniresultdir)
    drarChart(index, None, omniresultdir, olkfileDic, olkresultdir)


def drarChart(index, fileDicSecond, resultDirSecond, fileDicFirst, resultDirFirst):
    keys = list(fileDicFirst.keys())
    for key in keys:
        value = fileDicFirst.get(key)
        if fileDicSecond is None:
            olkValue=None
        else:
            olkValue = fileDicSecond.get(key)
        value.remove(key)
        if olkValue is None:
            secondList = [0, 0, 0, 0, 0, 0]
        else:
            olkValue.remove(key)
            secondList = [round(float(v) / 1000000, 2) for v in olkValue]  # trans ns to ms
            fileDicSecond.pop(key)
        firstList = [round(float(v) / 1000000, 2) for v in value]
        drawChart(index, key, secondList, firstList, resultDirFirst, resultDirSecond)
        fileDicFirst.pop(key)
        index += 1
    return index


def drawChart(index, key, olklist, omnilist, resultDirFirst, resultDirSecond):
    x = ['addInputWall', 'addInputCpu', 'getOutputWall', 'avg_getOutputCpu', 'avg_finishWall', 'avg_finishCpu']
    xlen = np.arange(len(x))
    width = 0.3
    plt.title(key + '(avg)')
    plt.plot(xlen + width / 8, omnilist, color='r', marker='o', label='omni')
    plt.plot(xlen + width, olklist, color='g', marker='o', label='olk')
    plt.bar(xlen, omnilist, width=width, color='r', label='omni', alpha=0.8)
    plt.bar(xlen + width, olklist, width=width, color='g', label='olk', alpha=0.8)
    for a, b in zip(xlen, omnilist):
        plt.text(a, b, b, ha='center', va='bottom', fontsize=10)
    for a, b in zip(xlen, olklist):
        plt.text(a, b, b, ha='center', va='bottom', fontsize=10)
    plt.legend()
    plt.xlabel('time item')
    plt.ylabel('operatortime(ms)')
    plt.xticks(range(len(x)), labels=x)
    # plt.show()
    plt.savefig(resultDirFirst + '/' + str(index) + '.png')
    plt.savefig(resultDirSecond + '/' + str(index) + '.png')
    plt.clf()


def ReadFileIntoDict(omnifile):
    fileDic={}
    fileList=[]
    data = open(omnifile)
    next(data)
    for line in data:
        line.replace('Omni', '')
        fileList.append(line.strip())
    for item in sorted(fileList):
        arr = item.split('\t')
        key = arr[0]
        fileDic[key] = arr
    return fileDic


def main(argv):
    if argv[1]=='queryInfo':
        queryInfo(argv[2],argv[3],argv[4],argv[5],argv[6])
    elif argv[1]=='comparefile':
        omnifile=argv[2]
        olkfile=argv[3]
        g=os.walk(argv[2])
        for path, dir_list, file_list in g:
            for dirname in dir_list:
                if dirname=='charter' or dirname=='json':
                    continue
                comparefile(omnifile+'/'+dirname+'/avg_result',olkfile+'/'+dirname+'/avg_result')
    elif argv[1]=='avgValue':
        filedir=argv[2]
        dir=os.listdir(filedir)
        for file_name in dir:
            if os.path.isfile(filedir+'/'+file_name):
                avgValue(filedir+'/'+file_name,filedir+'/avg_result')
    else:
        print('Parameter Error')

if __name__ == "__main__":
    main(sys.argv)
