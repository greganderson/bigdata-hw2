from pyspark import SparkContext, SparkConf
from numpy import matrix, array, empty

### CONFIGURATION ###

conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("reduce")
conf.set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)

blocksize = 1


#### CODE STARTS ###

f = sc.textFile('matrices/a_100x200.txt')
v = sc.textFile('matrices/xx_200.txt')

# Split then convert strings to numbers
mat_raw = f.map(lambda s: s.split(' ')).map(lambda row: (int(row[0]) / blocksize, [float(row[2])])).reduceByKey(lambda a,b: a+b).map(lambda (k,v): v)
vec_raw = v.map(lambda x: [float(x)])
vv = matrix(vec_raw.collect())
result = matrix(mat_raw.collect()) * vv

mat = empty([100, 200], dtype=float)
data = mat.collect()
for v in data:
	mat[v[0]][v[1]] = v[2]


matrix = sc.parallelize(mat.tolist())
vector = sc.parallelize(matrix(vec_raw.collect()).tolist())


with open('test.txt', 'w') as fl:
	fl.write(str(result))
