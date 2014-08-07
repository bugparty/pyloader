import os
import time
import re


def listdir(dir, prefix='result_spider_randomtopquery_url'):
    for item in os.listdir(dir):
        # print 'prf',prefix,'item',item
        if os.path.isdir(os.path.join(dir, item)) and item.find(prefix) >= 0:
            yield (os.path.join(dir, item))


def listfile(dir, subdirprefix='result_spider_randomtopquery_url',
             prefix='result_spider_deadlink_monitor_random_iphone_url'):
    for rootdir in listdir(dir, prefix=subdirprefix):
        for subdir in os.listdir(rootdir):
            if subdir.find(prefix) != -1:
                yield os.path.join(rootdir, subdir)


def parseDateString(dateStr):
    timeArray = time.strptime(dateStr, "%Y%m%d")
    return time.strftime('%Y-%m-%d', timeArray)


def stripDateStr(rawStr):
    reg = re.compile('.+\.\d{8}\.(\d{8})$')
    return reg.match(rawStr)


def splitline(line):
    splited = line.strip('\n').split('\t')
    if (len(splited) != 5):
        print >> sys.stderr, 'Fatal Error,bad format in file'
    return splited


def testStripDateStr():
    rawstr = 'result\result_spider_randomtopquery_url.20140723\result_spider_deadlink_monitor_random_iphone_url.20140723.20140723'
    print stripDateStr(rawstr)


def testListdir():
    ld = [sub for sub in listdir('result')]
    print 'ld', ld


def testListfile():
    subld = [sub for sub in listfile('result')]
    print 'subld', subld


if __name__ == '__main__':
    # testListdir()
    # testListfile()
    #print parseDateString("20140517")
    testStripDateStr()






