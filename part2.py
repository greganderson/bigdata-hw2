import sys
from pyspark import SparkContext, SparkConf

### CONFIGURATION ###

conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("reduce")
conf.set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)


#### CODE STARTS ###


def dot_product(x):
	a = x[1][0]
	b = x[1][1]
	total = 0
	for i in range(len(a)):
		total += a[i] * b[i]
	return (x[0], total)

if len(sys.argv) != 5:
	print 'Invalid arguments.'
	print 'Usage: spark-submit part1.py <matrix_file_1> <matrix_1_dimensions> <matrix_file_2> <matrix_2_dimensions>'
	exit(1)

file1 = sys.argv[1]

f = sc.textFile(file1)
g = sc.textFile(file1)

dim1 = sys.argv[2]
dim2 = sys.argv[2]
dim1 = dim1.replace('K', '000')
dim2 = dim2.replace('K', '000')
a_rows = int(dim1[:dim1.find('x')])
b_cols = int(dim2[dim2.find('x')+1:])


### Compute A^2

# Split then convert strings to numbers
a = f.map(lambda s: s.split(' ')).map(lambda row: (int(row[0]), int(row[1]), float(row[2])))
b = g.map(lambda s: s.split(' ')).map(lambda row: (int(row[0]), int(row[1]), float(row[2])))

# Map the values to their correct positions in the result matrix
r1 = a.flatMap(lambda (i, j, value): [((i, k), value) for k in range(b_cols)])
r2 = b.flatMap(lambda (j, k, value): [((i, k), value) for i in range(a_rows)])

x = r1.groupByKey().map(lambda (x, y): (x, list(y)))
y = r2.groupByKey().map(lambda (x, y): (x, list(y)))
z = x.join(y)

answer = z.map(dot_product)


### Add A

c = a.map(lambda (x, y, v): ((x, y), v))
d = c.join(answer)
e = d.map(lambda (k, v): (k, v[0]+v[1]))


### Find a zero

e.values()


r = answer.sortByKey().collect()
with open('result.txt', 'w') as fl:
	fl.write(str(r))
#answer.saveAsTextFile('result')
