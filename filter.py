from pyspark import SparkConf, SparkContext

conf1 = SparkConf()
conf1.setMaster("local")
conf1.setAppName("First program")
sc = SparkContext(conf = conf1)

Rdd = sc.textFile("users.txt")

def checkMale(x):
	newRecords = x.split(",")
	
	if newRecords[2] == 'Gender':
		return True
	elif newRecords[2] == 'M':
		return True
	else:
		return False

			
			
Rdd2 = Rdd.filter(checkMale)

data = Rdd2.collect()

for record in data:
	print(record)
	
	