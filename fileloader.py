# -*- coding: utf-8 -*-
import sys
import os
import codecs
import unittest
# third party libraries which you could get from pip
import chardet


class FileLoader():
    def __init__(self):
        self._raw_lines = []

    def get_encoding(self, filename):
        bytes = min(32, os.path.getsize(filename))
        raw = open(filename, 'rb').read(bytes)

        if raw.startswith(codecs.BOM_UTF8):
            encoding = 'utf-8-sig'
            print 'got utf8 bom'
        else:
            result = chardet.detect(raw)
            encoding = result['encoding']
            # print 'chardet result',result
        return encoding

    def _load(self, filename):
        'load raw_lines from a file'
        f = open(filename, 'rb')
        strs = f.read()
        if not strs:
            self._lines = []
            return []

        # print 'total bytes',len(strs)

        raw_lines = strs.split('\n')
        # print 'total raw_lines',len(raw_lines)

        detected_coding = self.get_encoding(filename).lower()

        try:
            if detected_coding != 'gb2312':
                #print >> sys.stderr, "Warning, the file may not encoded in gb2312"
                #print >> sys.stderr, "detected coding is ",detected_coding
                pass
        except AttributeError, e:
            if e.args[0] == "'NoneType' object has no attribute 'lower'":
                self._lines = []
                print >> sys.stderr, "File is empty,Skiped Reading"
        parsed_lines = [line.decode(detected_coding, 'ignore').encode('utf-8') for line in raw_lines]
        self._raw_lines = raw_lines
        #print 'total decoded raw_lines',len(raw_lines)
        #print self._raw_lines[0]
        # print 'the following line has error'
        # errorlist = []
        # for line in parsed_lines:
        #     if len(line.split('\t')) != 7:
        #         print 'got an error line'
        #         print 'line num:', parsed_lines.index(line), 'content:', line
        #
        #         errorlist.append(line)
        # for l in errorlist:
        #     parsed_lines.remove(l)

        self._lines = parsed_lines

        return parsed_lines

    def verify(self, columnCount):
        'verify lines if there are bad lines in deadlink file, warning: auto ingonre invlaid lines'
        if not self._lines:
            return False
        lines = []
        for line in self._lines:
            splited = line.split('\t')
            if len(splited) != columnCount:
                print 'verify error in:', splited
                print 'column count', len(splited)
            else:
                splited = [item.strip('\t').strip('\n') for item in splited]
                lines.append(splited)

        self._lines = lines

        return True

    def load(self, path, columnCount):
        self._load(path)
        if self.verify(columnCount):
            return self._lines


class FileLoaderUnitTest(unittest.TestCase):
    def setUp(self):
        self.loader = loader = FileLoader()

    def test_get_encoding(self):
        f1 = 'result/result_spider_deadlink_monitor_random_iphone_url.20140719.20140719'
        f2 = r'result\result_spider_random_classfiy_url.20140801\result_spider_deadlink_10000_aladdin.20140801'
        e1 = self.loader.get_encoding(f1)
        e2 = self.loader.get_encoding(f2)
        self.assertIsNotNone(e1)
        self.assertIsNotNone(e2)

    def test_fileLoader(self):
        loader = FileLoader()
        lines = loader._load('result/result_spider_deadlink_monitor_random_iphone_url.20140719.20140719')
        print 'loaded', len(lines), 'lines'
        isDataValid = loader.verify(7)
        self.assertTrue(isDataValid)

    def test__load1(self):
        loader = FileLoader()
        lines = loader._load(
            r'result\result_spider_random_classfiy_url.20140801\result_spider_deadlink_10000_aladdin.20140801')
        print 'loaded ', len(lines)
        self.assertGreater(len(lines), 0, "加载失败")

    def test__load_file2(self):
        loader = FileLoader()
        lines = loader._load(
            r'result\result_spider_randomtopquery_url.20140721\result_spider_deadlink_monitor_random_iphone_url.20140721.20140721')
        print 'loaded ', len(lines)
        self.assertGreater(len(lines), 0, "加载失败")

    def test__load_fileempty(self):
        loader = FileLoader()
        lines = loader._load(
            r'result\result_spider_randomtopquery_url.20140714\result_spider_deadlink_monitor_random_iphone_url.20140721.20140721')
        print 'loaded ', len(lines)

        self.assertEqual(len(lines), 0, "加载空文件没有行书")


if __name__ == '__main__':
    unittest.main()

