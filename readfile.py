filename1 = 'result_spider_deadlink_monitor_random_iphone_url.20140719.20140719'

f = open(filename1, 'rb')

strs = f.read()
print 'total bytes', len(strs)

lines = strs.split('\n')
print 'total lines', len(lines)

raw_lines = [line.decode('gb2312', 'ignore') for line in lines]

print 'total decoded lines', len(raw_lines)

print 'the following line has error'
for line in lines:
    if len(line.split('\t')) != 7:
        print 'got an error line'
        print 'line num:', lines.index(line), 'content:', line
        
