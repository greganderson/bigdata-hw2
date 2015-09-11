import commands
import sys

if len(sys.argv) != 2:
	print 'Invalid arguments.'
	print 'Usage: scrub_vector.py <filename>'
	exit(1)

filename = sys.argv[1]

s, o = commands.getstatusoutput('cat ' + filename)
# Extract the number from each line
lines = map(lambda line: line[line.find(' ')+1:line.find(']')], o.split('\n'))

with open('test.txt', 'w') as f:
	for num in lines:
		f.write(num + '\n')
