from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
cinf = SparkConf().setMaster("cluster")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()
print(result.items())
sortedResults = collections.OrderedDict(sorted(result.items()))
print(*sortedResults.items())
for key, value in sortedResults.items():
	print("%s %i" % (key, value))
