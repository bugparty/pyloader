# -*- coding: utf-8 -*-  
import sys
import os
import time
import codecs

import MySQLdb


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

        # print 'total bytes',len(strs)

        raw_lines = strs.split('\n')
        # print 'total raw_lines',len(raw_lines)
        if self.get_encoding(filename).lower() != 'gb2312':
            print >> sys.stderr, "Warning, the file may not encoded in gb2312"
        parsed_lines = [line.decode('gb2312', 'ignore').encode('utf-8') for line in raw_lines]
        self._raw_lines = raw_lines
        #print 'total decoded raw_lines',len(raw_lines)
        print self._raw_lines[0]
        print 'the following line has error'
        errorlist = []
        for line in parsed_lines:
            if len(line.split('\t')) != 7:
                print 'got an error line'
                print 'line num:', parsed_lines.index(line), 'content:', line
                errorlist.append(line)
        for l in errorlist:
            parsed_lines.remove(l)

        self._lines = parsed_lines

        return parsed_lines

    def verify(self):
        'verify lines if there are bad lines in deadlink file, warning: auto ingonre invlaid lines'
        if not self._lines:
            return False
        lines = []
        for line in self._lines:
            splited = line.split('\t')
            if len(splited) != 7:
                print 'verify error in:', splited
                print 'column count', len(splited)
                sys.exit(-1)
            splited = [item.strip('\t').strip('\n') for item in splited]
            lines.append(splited)

        self._lines = lines

        return True

    def load(self, path):
        self._load(path)
        if self.verify():
            return self._lines


class DB():
    def __init__(self):
        pass

    def _get_conn(self):
        try:
            conn = MySQLdb.connect('localhost', 'root', db='jiankong')
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            return
        return conn

    def _get_cur(self):
        conn = self._get_conn()
        cur = conn.cursor()
        return (conn, cur)

    def mysql_version(self):
        conn = self._get_conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT VERSION()")
            ver = cur.fetchone()[0]
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            sys.exit(1)
        finally:
            if conn:
                conn.close()

        return ver

    def drop_table_deadlink(self):
        _table = 'daily_deadlink'
        schema = ''' drop table %s''' % _table

        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(schema)
            print 'drop %s ' % _table
        except MySQLdb.OperationalError, e:
            print 'OperationalError', e.args[0], e.args[1]

        finally:
            if conn:
                conn.close()

    def create_table_deadlink(self):
        _table = 'daily_deadlink'
        schema_table_deadlink = ''' create table %s(
	 id int not null auto_increment,
	 query varchar(256),
	 type1 int,
	 dest_url varchar(1024),
	 src_url varchar(2048),
	 http_code int,
	 type2 varchar(32),
	 type3 varchar(32),
	 date date,
	 primary key(id)
	);
 ''' % _table

        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(schema_table_deadlink)
            print 'table %s created' % _table
        except MySQLdb.OperationalError, e:
            print 'OperationalError', e.args[0], e.args[1]
            if e.args[0] == 1050:
                print 'table exists,going to drop table %s' % _table
                self.drop_table_deadlink()
                self.create_table_deadlink()



        finally:
            if conn:
                conn.close()

    def inserts_deadlink(self, lines):
        schema = u"INSERT INTO daily_deadlink (query,type1,dest_url,src_url, http_code,type2,type3,date ) VALUES(%s, '%s')"
        # print 'line type',type(lines)
        lines = [("'" + "','".join(line) + "'").decode('u8') for line in lines]
        # print lines[:10]
        conn = self._get_conn()
        today = time.strftime('%Y-%m-%d')
        today = today.decode('u8')

        with conn:
            cur = conn.cursor()
            for line in lines:
                #line = line.decode('gb2312','ignore').encode('utf8')
                #print 'executing',schema % (line,today)
                q = schema % (line, today)
                enc_q = q.encode('u8')
                try:
                    cur.execute(enc_q)
                except MySQLdb.ProgrammingError, e:
                    if e.args[0] == 1064:
                        print "the query has syntax error", enc_q
                        print "the original query is", q

                    print >> sys.stderr, "Fatal Error, exit!"
                    sys.exit(2)

            conn.commit()
            print 'inserted ', len(lines), 'lines'


def testDbSimple():
    db = DB()
    print 'mysql version is', db.mysql_version()
    db.create_table_deadlink()

    db.drop_table_deadlink()
    db.create_table_deadlink()


def testFileLoader():
    loader = FileLoader()
    lines = loader._load('result_spider_deadlink_monitor_random_iphone_url.20140719.20140719')
    print 'loaded', len(lines), 'lines'
    isDataValid = loader.verify()
    print 'verified', isDataValid and 'OK' or 'failed'


def testDbInsert():
    loader = FileLoader()
    lines = loader.load(
        'result_spider_deadlink_monitor_random_iphone_url.20140723.20140723')
    db = DB()
    print 'staring insert lines'
    db.inserts_deadlink(lines)


def doTest():
    testFileLoader()
    testDbSimple()
    testDbInsert()


if __name__ == '__main__':
    if len(sys.argv) == 1:
        print '''Usage:
         %s result_file_path'''
        sys.exit(0)
    loader = FileLoader()
    print 'try loading', sys.argv[1]
    lines = loader.load(
        sys.argv[1])

    db = DB()
    db.create_table_deadlink()

    print 'staring insert lines'
    db.inserts_deadlink(lines)
    print 'insert completed'
    
    
    
               
    
    
