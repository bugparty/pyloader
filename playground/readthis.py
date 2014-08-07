import sys


def splitline(line):
    splited = line.strip('\n').split('\t')
    if (len(splited) != 5):
        print >> sys.stderr, 'Fatal Error,bad format in file'
    return splited


f = open('result_spider_10000_aladdin.20140801')

firstline = f.readline()
splited = firstline.strip('\n').split('\t')

splited = splitline(firstline)
splits = [splitline(split) for split in f.readlines()]

print len(splits)

stats = {}
print stats.has_key('200')
# print stats['200']

for split in splits:
    status = split[2]
    try:
        stats[status] += 1
    except KeyError, e:
        print 'stats does not has', e
        stats[status] = 1

print stats


