from pyspark.sql import SparkSession
import time

start = time.time()

spark = SparkSession.builder.appName("query3_rdd").getOrCreate()

sc = spark.sparkContext

#from ratings keep movie id and rating
dataset1 = sc.textFile("hdfs://master:9000/movie_data/ratings.csv", 100).map(lambda x: x.split(',')).map(lambda x: (int(x[1]), float(x[2])))

#from genres keep movie id and genre
dataset2 = sc.textFile("hdfs://master:9000/movie_data/movie_genres.csv").map(lambda x: x.split(',')).map(lambda x: (int(x[0]), x[1]))

#calculate the average rating of each movie
movie_avg = dataset1.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).mapValues(lambda x: x[0]/x[1]) 

#calculate the average rating of each genre and the amount of movies in it
genre_avg = dataset2.join(movie_avg).map(lambda x: (x[1][0], x[1][1])).mapValues(lambda x: (x, 1)).reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1])).mapValues(lambda x: (x[0]/x[1], x[1])).sortByKey()

for i in genre_avg.collect():
    print(i)

end = time.time()

print("Time:", (end-start))
