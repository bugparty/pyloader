# -*- encoding:utf8 -*-
import argparse
import itertools
import sys

from fileloader import FileLoader
from dbhelper import DB
import utils
import statistics


def do_import_rtq(db, password='', newfileonly=False):
    loader = FileLoader()
    db = DB(password=password, db=db)
    db.create_table_deadlink()
    # print 'try loading', sys.argv[1]
    # lines = loader.load(
    # sys.argv[1])
    newdate = str(db.get_last_day('deadlink'))

    if newdate == 'None':
        newfileonly = False
    for file in utils.dir_listfile('result'):
        rawdatestr = utils.stripDateStr(file).group(1)
        datestr = utils.parseDateString(rawdatestr)
        if newfileonly:
            if datestr > newdate:
                lines = loader.load(file, 7)
                print 'staring insert lines from', file, 'datetime is', datestr
                db.inserts_deadlink(lines, date=datestr)
                print 'insert completed'
            else:
                print datestr, 'skiped'
        else:
            lines = loader.load(file, 7)
            print 'staring insert lines from', file, 'datetime is', datestr
            db.inserts_deadlink(lines, date=datestr)
            print 'insert completed'


def do_import_rcu(db, password='', newfileonly=False):
    loader = FileLoader()
    db = DB(password=password, db=db)
    db.create_table_deadlink_classify()
    cates = ['aladdin', 'h5', 'lightaap', 'normal', 'siteapp', 'tc']
    newdate = str(db.get_last_day('deadlink'))

    if newdate == 'None':
        newfileonly = False

    def httpcodeNot200(line):
        return line[2] != '200'
    for category in cates:
        for file in utils.dir_listfile('result',
                                       subdirprefix='result_spider_random_classfiy_url',
                                       fileSubPrefix='result_spider_10000_' + category):
            datestr = utils.getDateFromStr(file)
            if newfileonly:
                if datestr > newfileonly:
                    lines = loader.load(file, 5)
                    try:
                        filteredlines = [i for i in itertools.ifilter(httpcodeNot200, lines)]
                    except TypeError, e:
                        print >> sys.stderr, e.args
                        continue
                    print 'deads', len(filteredlines), 'staring insert lines from', file, 'datetime is', datestr
                    db.inserts_deadlink_classify(filteredlines, cls=category, date=datestr)
                    print 'insert completed'
                else:
                    print datestr, 'skiped'
            else:
                lines = loader.load(file, 5)
                try:
                    filteredlines = [i for i in itertools.ifilter(httpcodeNot200, lines)]
                except TypeError, e:
                    print >> sys.stderr, e.args
                    continue
                print 'deads', len(filteredlines), 'staring insert lines from', file, 'datetime is', datestr
                db.inserts_deadlink_classify(filteredlines, cls=category, date=datestr)
                print 'insert completed'

def do_rcu_stat():
    loader = FileLoader()
    cates = ['aladdin', 'h5', 'lightaap', 'normal', 'siteapp', 'tc']
    for category in cates:
        for file in utils.dir_listfile('result',
                                       subdirprefix='result_spider_random_classfiy_url',
                                       fileSubPrefix='result_spider_10000_' + category):
            datestr = utils.getDateFromStr(file)
            lines = loader.load(file, 5)
            print 'date is', datestr, 'file is', file
            stats = statistics.stat_httpcode(lines, 2)

            print 'stat is', stats


def do_test(db, password=''):
    db = DB(password=password, db=db)
    ret = db.get_last_day('classify')
    print 'last day is', ret


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("cmd", choices=['rtq', 'rcu', 'rcu_stat', 'test'],
                        help=u"指定运行的命令,导入randomTopQuery,请使用rtq."
                             u"导入random query classfiy死链数据,请使用rcu"
                             u"生成random query classify统计数据,使用rcu_stat")
    parser.add_argument("--verbose", action="store_true", help=u"输出全部信息")
    dbname_default = 'deadlink_monitor'
    dbpwd_default = 'wisetest'
    parser.add_argument("--dbpwd", help=u"指定数据库密码,默认为" + dbpwd_default, default=dbpwd_default)
    parser.add_argument("--dbname", help=u"指定数据库名,默认为" + dbname_default, default=dbname_default)
    parser.add_argument("-N", action="store_true", help=u"增量更新")

    args = parser.parse_args()

    if (args.cmd == 'rtq'):
        do_import_rtq(db=args.dbname, password=args.dbpwd, newfileonly=args.N)
    elif (args.cmd == 'rcu'):
        do_import_rcu(db=args.dbname, password=args.dbpwd, newfileonly=args.N)
    elif (args.cmd == 'rcu_stat'):
        do_rcu_stat()
    elif (args.cmd == 'test'):
        do_test(db=args.dbname, password=args.dbpwd)







