from pyspark import SparkContext, SparkConf
from numpy import matrix, array

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

v_rdd = v.map(lambda x: [float(x)])
vv = matrix(v_rdd.collect())
# Split then convert strings to numbers
rdd = f.map(lambda s: s.split(' ')).map(lambda row: (int(row[0]) / blocksize, [float(row[2])])).reduceByKey(lambda a,b: a+b).map(lambda (k,v): v)

result = matrix(rdd.collect()) * vv

with open('test.txt', 'w') as fl:
	fl.write(str(result))
