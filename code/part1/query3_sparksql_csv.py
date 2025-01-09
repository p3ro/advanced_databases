from pyspark.sql import SparkSession
import time

start = time.time()

spark = SparkSession.builder.appName("query3_sparksql_csv").getOrCreate()

ratings = spark.read.format('csv').\
    options(header = 'false', inferSchema = 'true').\
    load("hdfs://master:9000/movie_data/ratings.csv")

ratings.registerTempTable("ratings")

genres = spark.read.format('csv').\
    options(header = 'false', inferSchema = 'true').\
    load("hdfs://master:9000/movie_data/movie_genres.csv")

genres.registerTempTable("genres")

sqlString = "select Genre, avg(Movie_Avg_Rating) as Genre_Avg_Rating, count(Movie_Avg_Rating) as Num_Of_Movies " + \
    "from (select _c0 as ID, _c1 as Genre from genres) as g, (select _c1 as ID, avg(_c2) as Movie_Avg_Rating from ratings group by ID order by ID) as movie_avg " + \
    "where g.ID = movie_avg.ID " + \
    "group by Genre order by Genre"

res = spark.sql(sqlString)

res.show()

end = time.time()

print("Time:", (end-start))
