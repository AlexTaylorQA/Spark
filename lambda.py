from pyspark import SparkConf, SparkContext

conf1 = SparkConf()
conf1.setMaster("local")
conf1.setAppName("First program")
sc = SparkContext(conf = conf1)

list1 = [1,2,10,75,5,49,95]

Rdd = sc.parallelize(list1)
					
Rdd2 = Rdd.filter(lambda x: x > 10)

data = Rdd2.collect()

for record in data:
	print(record)
	
	