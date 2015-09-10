from pyspark import SparkContext, SparkConf
import json


conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("reduce")
conf.set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)


def func(x):
	row = x.split(' ')
	if row[0] not in mat:
		mat[row[0]] = {}
	mat[row[0]] = {row[1] : row[2]}

f = sc.textFile('matrices/a_100x200.txt')

mat = {}
rdd = f.map(func)

result = json.dumps(rdd.collect())

with open('test.txt', 'w') as fl:
	fl.write(result)
