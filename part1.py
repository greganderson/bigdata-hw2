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
v = sc.textFile('matrices/xx_100.txt')

v_rdd = v.map(lambda x: float(x))
vv = matrix(v_rdd.collect())
# Split then convert strings to numbers
<<<<<<< HEAD
rdd = f.map(lambda s: s.split(' ')).map(lambda row: (int(row[0]) / blocksize, [float(row[2])])).reduceByKey(lambda a,b: a+b).map(lambda (k,v): v)
=======
rdd = f.map(lambda s: s.split(' ')) \
			 .map(lambda row: (int(row[0]) / blocksize, [int(row[0]), int(row[1]), float(row[2])])) \
			 .groupByKey()
>>>>>>> 767ed134f42c77e614789666031bc17ac714f169

#result = rdd.collect()
result = vv * matrix(rdd.collect())

with open('test.txt', 'w') as fl:
	fl.write(str(result))
