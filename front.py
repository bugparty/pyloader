# -*- encoding:utf8 -*-
import argparse

from fileloader import FileLoader
from dbhelper import DB
import utils
import statistics


def do_import_rtq(password=''):
    loader = FileLoader()
    db = DB(password=password)
    db.create_table_deadlink()
    # print 'try loading', sys.argv[1]
    # lines = loader.load(
    #     sys.argv[1])
    for file in utils.listfile('result'):
        rawdatestr = utils.stripDateStr(file).group(1)
        datestr = utils.parseDateString(rawdatestr)
        lines = loader.load(file, 7)
        print 'staring insert lines from', file, 'datetime is', datestr

        db.inserts_deadlink(lines, date=datestr)
        print 'insert completed'


def do_import_rcu(password=''):
    loader = FileLoader()
    db = DB(password=password)
    db.create_table_deadlink_classify()

    for file in utils.listfile('result',
                               subdirprefix='result_spider_random_classfiy_url',
                               fileSubPrefix='result_spider_10000_aladdin'):
        datestr = utils.getDateFromStr(file)
        lines = loader.load(file, 5)
        print 'staring insert lines from', file, 'datetime is', datestr
        db.inserts_deadlink_classify(lines, cls='aladdin', date=datestr)
        print 'insert completed'


def do_rcu_stat():
    loader = FileLoader()
    cates = ['aladdin', 'h5', 'lightaap', 'normal', 'siteapp', 'tc']
    for category in cates:
        for file in utils.listfile('result',
                                   subdirprefix='result_spider_random_classfiy_url',
                                   fileSubPrefix='result_spider_10000_' + category):
            datestr = utils.getDateFromStr(file)
            lines = loader.load(file, 5)
            print 'date is', datestr, 'file is', file
            stats = statistics.stat_httpcode(lines, 2)

            print 'stat is', stats



if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("cmd", choices=['rtq', 'rcu', 'rcu_stat'],
                        help=u"指定运行的命令,导入randomTopQuery,请使用rtq.导入classfiy_url,请使用rcu")
    parser.add_argument("--verbose", action="store_true", help="输出全部信息")
    parser.add_argument("--dbpwd", help="指定数据库密码,默认为空")

    args = parser.parse_args()

    if (args.cmd == 'rtq'):
        if (args.dbpwd):
            do_import_rtq(password=args.dbpwd)
        else:
            do_import_rtq()

    elif (args.cmd == 'rcu'):
        if (args.dbpwd):
            do_import_rcu(password=args.dbpwd)
        else:
            do_import_rcu()
    elif (args.cmd == 'rcu_stat'):
        do_rcu_stat()







