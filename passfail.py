from pyspark import SparkConf, SparkContext

conf1 = SparkConf()
conf1.setMaster("local")
conf1.setAppName("First program")
sc = SparkContext(conf = conf1)

Rdd = sc.textFile("users.txt")

def passfail(x):
	newRecords = x.split(",")
	
	thePercent = ""
	remarks = ""
	if newRecords[4] == 'Marks':
		remarks = "Result"
		thePercent = "Percentage"
	else:
		thePercent = int(newRecords[4])*100/150
		if thePercent >= 60:
			remarks = "Pass"
		else:
			remarks = "Fail"
		
	return (newRecords[0], newRecords[1], newRecords[2], newRecords[3], newRecords[4], thePercent, remarks)

Rdd1 = Rdd.map(passfail)

data = Rdd1.collect()

for record in data:
	print(record)
	
