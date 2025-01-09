from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

start = time.time()

#given function for reading movies.csv file
def split_complex(x):
	return list(csv.reader(StringIO(x), delimiter=','))[0]

#calculates the profit of a film
def profit(income, outcome):
	inc = int(income)
	out = int(outcome)
	return (inc-out)*100/out	

#returns the year portion of a timestamp
def get_year(ts):
	return int(ts[:4])	

spark = SparkSession.builder.appName("query1-with_rdd").getOrCreate()

sc = spark.sparkContext

dataset = sc.textFile("hdfs://master:9000/movie_data/movies.csv").\
	map(lambda x: split_complex(x)).\
	filter(lambda x: not(int(x[5]) == 0 or int(x[6]) == 0 or x[3] == "")).\
	filter(lambda x: not(get_year(x[3]) < 2000)).\
	map(lambda x: (get_year(x[3]),(x[1],profit(x[6], x[5])))).\
	reduceByKey(lambda x, y: (x[0] if x[1] > y[1] else y[0], x[1] if x[1] > y[1] else y[1])).\
	sortByKey()

for i in dataset.collect():
	print(i)

end = time.time()

print("Time:", (end-start))
