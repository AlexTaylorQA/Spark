from pyspark import SparkConf, SparkContext

conf1 = SparkConf()
conf1.setMaster("local")
conf1.setAppName("First program")
sc = SparkContext(conf = conf1)

list1 = [1,2,3,4,5,6,7]
list2 = [6,7,8,9,10]

Rdd1 = sc.parallelize(list1)
Rdd2 = sc.parallelize(list2)

