from pyspark import SparkConf, SparkContext

conf1 = SparkConf()
conf1.setMaster("local")
conf1.setAppName("First program")
sc = SparkContext(conf = conf1)

Rdd = sc.textFile("users.txt")

theteens = "ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen".split()

numbers = "zero one two three four five six seven eight nine".split()
numbers.extend("ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen".split())
numbers.extend(tens if ones == "zero" else (tens + "-" + ones)
	for tens in "twenty thirty forty fifty sixty seventy eighty ninety".split()
    for ones in numbers[0:10])

	
numbers.extend(
(hundreds + " hundred") if tens == "zero" and ones == "zero" 
else (hundreds + " hundred and " + tens) if ones == "zero" 
else (hundreds + " hundred and " + theteens[numbers.index(ones)]) if tens == "ten" and ones != "zero"  
else (hundreds + " hundred and " + tens + "-" + ones)


	for hundreds in numbers[1:10]
	for tens in "zero ten twenty thirty forty fifty sixty seventy eighty ninety".split()
    for ones in numbers[0:10])


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
			theWord = numbers[int(newRecords[4])].capitalize()
		else:
			remarks = "Fail"
			theWord = numbers[int(newRecords[4])].capitalize()
		
	return (newRecords[0], newRecords[1], newRecords[4], theWord, remarks)

Rdd1 = Rdd.map(passfail)

data = Rdd1.collect()

for record in data:
	print(record)
	
