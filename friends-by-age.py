from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)


def parseLine(line):
	fields = line.split(',')
	age = int(fields[2])
	numFriends = int(fields[3])
	return age, numFriends


def parseName(line):
	fields = line.split(',')
	name = str(fields[1])
	return name


lines = sc.textFile("file:///SparkCourse/fakefriends.csv")
rdd = lines.map(parseLine)
rdd1 = lines.map(parseName)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
mapByName = rdd1.map(lambda x: (x, 1))
print(mapByName.collect())
totalsByName = mapByName.reduceByKey(lambda x, y: x + y)
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()
print(totalsByName.collect())
print(results)
