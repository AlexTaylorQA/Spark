from pyspark import SparkConf, SparkContext
from num2words import num2words

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
		theWord = "MarksText"
	else:
		thePercent = int(newRecords[4])*100/150
		if thePercent >= 60:
			remarks = "Pass"
			theWord = num2words(int(newRecords[4])).capitalize()
		else:
			remarks = "Fail"
			theWord = num2words(int(newRecords[4])).capitalize()
		
	return (newRecords[0], newRecords[1], newRecords[4], theWord, remarks)

Rdd1 = Rdd.map(passfail)

data = Rdd1.collect()

for record in data:
	print(record)
	
