from pyspark.sql import SparkSession
import datetime
import time

start = time.time()

spark = SparkSession.builder.appName("query1_sparksql_csv").getOrCreate()

movies = spark.read.format('csv').\
    options(header = 'false', inferSchema = 'true').\
    load("hdfs://master:9000/movie_data/movies.csv")

def get_year(ts):
    return ts.year

movies.registerTempTable("movies")
spark.udf.register("get_year", get_year)

sqlString = "select n.Year, m.Title, n.Profit " + \
    "from (select year(_c3) as Year, max((_c6-_c5)*100/_c5) as Profit " + \
    "from movies where _c3 is not null and year(_c3) >= 2000 and _c5 <> 0 and _c6 <> 0 " + \
    "group by Year order by Year) as n, (select year(_c3) as Year, _c1 as Title, (_c6-_c5)*100/_c5 as Profit " + \
    "from movies where _c3 is not null and year(_c3) >= 2000 and _c5 <> 0 and _c6 <> 0) as m " + \
    "where n.Profit = m.Profit and n.Year = m.Year order by n.Year"

res = spark.sql(sqlString)

res.show()

end = time.time()

print("Time:", (end-start))	  
