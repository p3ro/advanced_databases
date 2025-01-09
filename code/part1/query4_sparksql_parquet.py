from pyspark.sql import SparkSession
import time

start = time.time()

def get_quinquennium(ts):
    year = ts.year
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

def get_year(ts):
   return ts.year

def word_count(summ):
    return len(list(summ.split(" ")))

spark = SparkSession.builder.appName("query4_sparksql_parquet").getOrCreate()

spark.udf.register("get_year", get_year)
spark.udf.register("get_quinquennium", get_quinquennium)
spark.udf.register("word_count", word_count)


movies = spark.read.parquet("hdfs://master:9000/movie_data/movies.parquet")

movies.registerTempTable("movies")

genres = spark.read.parquet("hdfs://master:9000/movie_data/movie_genres.parquet")

genres.registerTempTable("genres")

sqlString = "select Quinquennium, sum(Summary)/count(Summary) as Avg_Length " + \
    "from (select (_c0) as ID, (_c1) as Genre from genres where _c1 = 'Drama') as d, (select _c0 as ID, word_count(_c2) as Summary, get_quinquennium(_c3) as Quinquennium " + \
    "from movies where get_year(_c3) >= 2000 and _c3 is not Null and _c2 is not Null) as m where d.ID = m.ID group by Quinquennium order by Quinquennium"

res = spark.sql(sqlString)

res.show()

end = time.time()

print("Time:", (end - start))
