# -*- encoding:utf8 -*-
import argparse

from deadlink import FileLoader, DB
import utils


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
        lines = loader.load(file)
        print 'staring insert lines from', file, 'datetime is', datestr

        db.inserts_deadlink(lines, date=datestr)
        print 'insert completed'


def do_import_rcu(password=''):
    loader = FileLoader()
    db = DB(password=password)
    db.create_table_deadlink()
    # print 'try loading', sys.argv[1]
    # lines = loader.load(
    #     sys.argv[1])
    for file in utils.listfile('result'):
        rawdatestr = utils.stripDateStr(file).group(1)
        datestr = utils.parseDateString(rawdatestr)
        lines = loader.load(file)
        print 'staring insert lines from', file, 'datetime is', datestr

        db.inserts_deadlink(lines, date=datestr)
        print 'insert completed'


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("cmd", choices=['rtq', 'rcu'],
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






