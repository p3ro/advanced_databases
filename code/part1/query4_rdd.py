from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

start = time.time()

spark = SparkSession.builder.appName("query4_rdd").getOrCreate()

sc = spark.sparkContext

#given function for readint movies.csv
def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=","))[0]

#returns the quinquennium of the timestamp or 0 if the year is earilier than 2000 
def get_quinquennium(ts):
    year = int(ts[:4])
    if year >= 2000 and year <= 2004:
        return 2000
    elif year >= 2005 and year <= 2009:
        return 2005
    elif year >= 2010 and year <= 2014:
        return 2010
    elif year >= 2015 and year <= 2019:
        return 2015
    else:
        return 0

#returns the length of the summary
def word_count(summ):
    return len(list(summ.split(" ")))

#from movies.csv keep the id, the length of the summary and the quinquennium of movies released after 2000 
movies = sc.textFile("hdfs://master:9000/movie_data/movies.csv").\
    map(lambda x: split_complex(x)).\
    filter(lambda x: not(x[3] == None or x[3] == "" or get_quinquennium(x[3]) < 2000 or x[2] == "")). \
    map(lambda x: (int(x[0]), (word_count(x[2]), get_quinquennium(x[3]))))

#from movie_genres.csv keep the id and the id of all Drama movies
movie_genres = sc.textFile("hdfs://master:9000/movie_data/movie_genres.csv"). \
    map(lambda x: x.split(',')). \
    filter(lambda x: x[1] == "Drama"). \
    map(lambda x: (int(x[0]), x[1]))

#join movies and movie_genres based on id and then find the average length of the summary of Drama movies per quinquennium
dramasum = movies.join(movie_genres). \
    map(lambda x: (x[1][0][1], x[1][0][0])). \
    mapValues(lambda x: (x, 1)). \
    reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])). \
    mapValues(lambda x: (x[0]/x[1])). \
    sortByKey() 


for i in dramasum.collect():
    print(i)

end = time.time()

print("Time:", (end - start))
