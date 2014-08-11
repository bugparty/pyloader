import os
import time
import re
import unittest


def listdir(dir, dirSubPrefix='result_spider_randomtopquery_url'):
    for item in os.listdir(dir):
        # print 'prf',prefix,'item',item
        if os.path.isdir(os.path.join(dir, item)) and item.find(dirSubPrefix) >= 0:
            yield (os.path.join(dir, item))


def listfile(dir, subdirprefix='result_spider_randomtopquery_url',
             fileSubPrefix='result_spider_deadlink_monitor_random_iphone_url'):
    for rootdir in listdir(dir, dirSubPrefix=subdirprefix):
        for subdir in os.listdir(rootdir):
            if subdir.find(fileSubPrefix) != -1:
                yield os.path.join(rootdir, subdir)


def parseDateString(dateStr):
    timeArray = time.strptime(dateStr, "%Y%m%d")
    return time.strftime('%Y-%m-%d', timeArray)


def stripDateStr(rawStr):
    reg = re.compile('.+\.(\d{8})$')
    return reg.match(rawStr)


def splitline(line):
    splited = line.strip('\n').split('\t')
    if (len(splited) != 5):
        print >> sys.stderr, 'Fatal Error,bad format in file'
    return splited


def getDateFromStr(rawStr):
    if stripDateStr(rawStr):
        return parseDateString(stripDateStr(rawStr).group(1))


class TestUtilFunctions(unittest.TestCase):
    def setUp(self):
        pass

    def test_stripDateStr(self):
        rawstr1 = 'result\result_spider_randomtopquery_url.20140723\result_spider_deadlink_monitor_random_iphone_url.20140723.20140723'
        rawstr2 = 'C:\Users\hanbowen\PycharmProjects\pyloader\result\result_spider_random_classfiy_url.20140801\result_spider_deadlink_10000_aladdin.20140801'

        self.assertIsNotNone(stripDateStr(rawstr1))
        self.assertIsNotNone(stripDateStr(rawstr2))

    def test_stripDateStr(self):
        rawstr1 = 'result\result_spider_randomtopquery_url.20140723\result_spider_deadlink_monitor_random_iphone_url.20140723.20140723'
        rawstr2 = 'C:\Users\hanbowen\PycharmProjects\pyloader\result\result_spider_random_classfiy_url.20140801\result_spider_deadlink_10000_aladdin.20140801'

        self.assertEqual('2014-07-23', getDateFromStr(rawstr1))
        self.assertEqual('2014-08-01', getDateFromStr(rawstr2))

    def test_listdir(self):
        ld = [sub for sub in listdir('result')]
        self.assertTrue(ld)


    def test_listfile(self):
        subld = [sub for sub in listfile('result')]
        self.assertTrue(subld)


if __name__ == '__main__':
    # testListdir()
    # test_listfile()
    #print parseDateString("20140517")
    unittest.main()






