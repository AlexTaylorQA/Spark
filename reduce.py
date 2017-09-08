from pyspark import SparkConf, SparkContext

conf1 = SparkConf()
conf1.setMaster("local")
conf1.setAppName("First program")
sc = SparkContext(conf = conf1)

def add(x,y):
	return x + y
	
Rdd = sc.parallelize([1,2,3,4,5])

data = Rdd.reduce(add)

print(data)
	