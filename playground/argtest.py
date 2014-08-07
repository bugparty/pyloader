import sys

# print sys.argc
#print sys.argv
if len(sys.argv) == 1:
    print 'Usage: blablabla'
    sys.exit(0)
print 'para1', sys.argv[1]

