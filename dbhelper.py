# -*- coding: utf-8 -*-
import sys
import time
import unittest
import traceback

import MySQLdb
import _mysql_exceptions

from fileloader import FileLoader

ERRORCODE_DB_AUTH_FAIL = 3


class DB():
    _table_deadlink = 'think_deadlink'
    _table_deadlink_classify = 'think_deadlink_classify'

    _schema_table_deadlink = ''' create table %s(
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
 ''' % _table_deadlink
    _schema_table_deadlink_classify = ''' create table %s(
	 id int not null auto_increment,
	 dest_url varchar(1024),
	 src_url varchar(2048),
	 http_code int,
	 page_weight varchar(32),
	 dead_reason varchar(32),
	 class varchar(32),
	 date date,
	 primary key(id)
	);
 ''' % _table_deadlink_classify

    _schema_drop_deadlink = ''' drop table %s''' % _table_deadlink
    _schema_drop_deadlink_classify = ''' drop table %s''' % _table_deadlink_classify

    _schema_insert_table_deadlink = u"INSERT INTO think_deadlink (query,type1,dest_url,src_url, http_code,type2,type3,date ) VALUES(%s, '%s')"
    _schema_insert_table_deadlink_classify = u"INSERT INTO think_deadlink_classify (dest_url,src_url, http_code,page_weight,dead_reason,class,date ) VALUES(%s,'%s', '%s')"

    def __init__(self, user='root', password='', host='localhost', db='jiankong'):
        self.user = user
        self.password = password
        self.host = host
        self.db = db


    def _get_conn(self):
        try:
            if (self.user == 'root' and self.password == '' and self.host == 'localhost'):
                conn = MySQLdb.connect('localhost', 'root', db='jiankong', charset='utf8')
            else:
                conn = MySQLdb.connect(self.host, self.user, self.password, db=self.db, charset='utf8')
        except MySQLdb.Error, e:
            print >> sys.stderr, "Error %d: %s" % (e.args[0], e.args[1])

            sys.exit(ERRORCODE_DB_AUTH_FAIL)
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
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(self._schema_drop_deadlink)
            print 'drop %s ' % self._table_deadlink
        except MySQLdb.OperationalError, e:
            print 'OperationalError', e.args[0], e.args[1]

        finally:
            if conn:
                conn.close()

    def create_table_deadlink(self):


        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(self._schema_table_deadlink)
            print 'table %s created' % self._table_deadlink
        except MySQLdb.OperationalError, e:
            print 'OperationalError', e.args[0], e.args[1]
            if e.args[0] == 1050:
                print 'table exists,going to drop table %s' % self._table_deadlink
                self.drop_table_deadlink()
                self.create_table_deadlink()



        finally:
            if conn:
                conn.close()

    def inserts_deadlink(self, lines, date='today'):

        print 'line type', type(lines)
        try:
            lines = [("'" + "','".join(line) + "'").decode('u8') for line in lines]
        except TypeError, e:
            print >> sys.stderr, e.args[0]
            if e.args[0] == "'NoneType' object is not iterable":
                print >> sys.stderr, 'Empty file, skiped'
            return
        # print lines[:10]
        conn = self._get_conn()
        if date == 'today':
            today = time.strftime('%Y-%m-%d')
            data_date = today.decode('u8')
        else:
            data_date = date

        with conn:
            cur = conn.cursor()
            for line in lines:
                # line = line.decode('gb2312','ignore').encode('utf8')
                #print 'executing',schema % (line,today)
                q = self._schema_insert_table_deadlink % (line, data_date)
                enc_q = q.encode('u8')
                try:
                    cur.execute(enc_q)
                except MySQLdb.ProgrammingError, e:
                    if e.args[0] == 1064:
                        print "the query has syntax error", enc_q
                        print "the original query is", q
                    else:
                        print e.args
                        print >> sys.stderr, "Fatal Error, exit!"
                    sys.exit(2)

            conn.commit()
            print 'inserted ', len(lines), 'lines'

    def drop_table_deadlink_classify(self):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(self._schema_drop_deadlink_classify)
            print 'drop %s ' % self._table_deadlink_classify
        except MySQLdb.OperationalError, e:
            print 'OperationalError', e.args[0], e.args[1]

        finally:
            if conn:
                conn.close()

    def create_table_deadlink_classify(self):

        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(self._schema_table_deadlink_classify)
            print 'table %s created' % self._table_deadlink_classify
        except MySQLdb.OperationalError, e:
            print 'OperationalError', e.args[0], e.args[1]
            if e.args[0] == 1050:
                print 'table exists,going to drop table %s' % self._table_deadlink_classify
                self.drop_table_deadlink_classify()
                self.create_table_deadlink_classify()

        finally:
            if conn:
                conn.close()

    def inserts_deadlink_classify(self, lines, cls, date='today'):

        print 'line type', type(lines)
        try:
            lines = [('"' + '","'.join(line) + '"').decode('u8') for line in lines]
            # lines = [("'" + "','".join(line) + "'").decode('u8') for line in lines]
        except TypeError, e:
            print >> sys.stderr, e.args[0]
            if e.args[0] == "'NoneType' object is not iterable":
                print >> sys.stderr, 'Empty file, skiped'
            return
        # print lines[:10]
        conn = self._get_conn()
        if date == 'today':
            today = time.strftime('%Y-%m-%d')
            data_date = today.decode('u8')
        else:
            data_date = date

        with conn:
            cur = conn.cursor()
            for line in lines:
                # line = line.decode('gb2312','ignore').encode('utf8')
                #print 'executing',schema % (line,today)
                q = self._schema_insert_table_deadlink_classify % (line, cls, data_date)
                enc_q = q.encode('u8')
                try:
                    cur.execute(enc_q)
                except MySQLdb.ProgrammingError, e:
                    if e.args[0] == 1064:
                        print "the query has syntax error", enc_q
                        print "the original query is", q
                    else:
                        print e.args
                        print >> sys.stderr, "Fatal Error, exit!"
                    sys.exit(2)
                except _mysql_exceptions.OperationalError, e:
                    print e.args
                    print 'the query is', enc_q
                    traceback.print_exc()
                    sys.exit(3)

            conn.commit()
            print 'inserted ', len(lines), 'lines'


class DBUnitTest(unittest.TestCase):
    def setUp(self):
        self.db = DB(password='root')

    def test_mysql_version(self):
        self.assertIsNotNone(self.db.mysql_version())

    def test_create_table_deadlink(self):
        self.db.create_table_deadlink()
        self.db.drop_table_deadlink()
        self.db.create_table_deadlink()

    def test_create_table_deadlink_classify(self):
        self.db.create_table_deadlink_classify()
        self.db.drop_table_deadlink_classify()
        self.db.create_table_deadlink_classify()

    def test_dbInsert(self):
        loader = FileLoader()
        lines = loader.load(
            'result/result_spider_deadlink_monitor_random_iphone_url.20140719.20140719', 7)
        print 'staring insert lines'
        self.db.inserts_deadlink(lines)


if __name__ == '__main__':
    unittest.main()
