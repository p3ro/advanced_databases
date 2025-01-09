from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

start = time.time()

#given function for reading movies.csv file
def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

spark = SparkSession.builder.appName("query5_rdd").getOrCreate()

sc = spark.sparkContext

#from movies keep movie id, movie name and popularity
movies = sc.textFile("hdfs://master:9000/movie_data/movies.csv").map(lambda x: split_complex(x)).map(lambda x: (int(x[0]), (x[1], float(x[7]))))

#from ratings keep movie id, user id and rating
reviews = sc.textFile("hdfs://master:9000/movie_data/ratings.csv", 100).map(lambda x: x.split(',')).map(lambda x: (int(x[1]), (int(x[0]), float(x[2]))))

#from genres keep movie id and genre
genres = sc.textFile("hdfs://master:9000/movie_data/movie_genres.csv").map(lambda x: x.split(',')).map(lambda x: (int(x[0]), x[1]))

#first create a dataset with genre-userid as the key and then two tuples where we will find the best and worst rated movie for each user-genre and also the no of reviews each user has in each genre
detailed_reviews = reviews.join(genres).join(movies).map(lambda x: ((x[1][0][1], x[1][0][0][0]), ((x[0], x[1][1][0], x[1][0][0][1], x[1][1][1]), (x[0], x[1][1][0], x[1][0][0][1], x[1][1][1])))). \
    mapValues(lambda x: (x,1)). \
    reduceByKey(lambda x, y: ((((x[0][0][0], x[0][0][1], x[0][0][2], x[0][0][3]), (x[0][1][0], x[0][1][1], x[0][1][2], x[0][1][3])), x[1] + y[1]) \
    if ((x[0][0][2] > y[0][0][2] or (x[0][0][2] == y[0][0][2] and x[0][0][3] > y[0][0][3])) and (x[0][1][2] < y[0][1][2] or (x[0][1][2] == y[0][1][2] and x[0][1][3] > y[0][1][3]))) \
    else (((x[0][0][0], x[0][0][1], x[0][0][2], x[0][0][3]), (y[0][1][0], y[0][1][1], y[0][1][2], y[0][1][3])), x[1] + y[1]) \
    if ((x[0][0][2] > y[0][0][2] or (x[0][0][2] == y[0][0][2] and x[0][0][3] > y[0][0][3])) and (y[0][1][2] < x[0][1][2] or (x[0][1][2] == y[0][1][2] and y[0][1][3] > x[0][1][3]))) \
    else (((y[0][0][0], y[0][0][1], y[0][0][2], y[0][0][3]), (x[0][1][0], x[0][1][1], x[0][1][2], x[0][1][3])), x[1] + y[1]) \
    if ((y[0][0][2] > x[0][0][2] or (x[0][0][2] == y[0][0][2] and y[0][0][3] > x[0][0][3])) and (x[0][1][2] < y[0][1][2] or (x[0][1][2] == y[0][1][2] and x[0][1][3] > y[0][1][3]))) \
    else (((y[0][0][0], y[0][0][1], y[0][0][2], y[0][0][3]), (y[0][1][0], y[0][1][1], y[0][1][2], y[0][1][3])), x[1] + y[1])))

#then we keep for each genre the user with the most reviews in it and his most and least favourite movie from the genre as found before
pro_reviews = detailed_reviews.map(lambda x: (x[0][0], (x[0][1], x[1][1], (x[1][0][0][1], x[1][0][0][2]), (x[1][0][1][1], x[1][0][1][2])))). \
    reduceByKey(lambda x, y: ((x[0], x[1], (x[2][0], x[2][1]), (x[3][0], x[3][1])) if x[1] > y[1] else (y[0], y[1], (y[2][0], y[2][1]), (y[3][0], y[3][1])))). \
    sortByKey()

for i in pro_reviews.collect():
    print(i)

end = time.time()

print("Time:", (end - start))
