from pyspark.sql import SparkSession
import time

start = time.time()

spark = SparkSession.builder.appName("query2_rdd").getOrCreate()

sc = spark.sparkContext

dataset = sc.textFile("hdfs://master:9000/movie_data/ratings.csv", 100).map(lambda x: x.split(',')).map(lambda x: (int(x[0]), float(x[2])))

#calculate the avg rating of each user
dataset = dataset.mapValues(lambda x: (x,1)).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])).mapValues(lambda x: x[0]/x[1]).sortByKey()

#find the total number of users
num_of_users = dataset.count()

#find the total number of users with an avg rating over 3
num_of_users_with_av_rating_over3 = dataset.filter(lambda x: x[1] >= 3.0).count()

print("Percentage of users with average movie rating over 3: ", num_of_users_with_av_rating_over3*100/num_of_users, "%", sep="")

end = time.time()

print("Time:", (end - start))
