from pyspark.sql import SparkSession
import time

start = time.time()

spark = SparkSession.builder.appName("query2_sparksql_csv").getOrCreate()

ratings = spark.read.format('csv').\
    options(header = 'false', inferSchema = 'true').\
    load("hdfs://master:9000/movie_data/ratings.csv")

ratings.registerTempTable("ratings")

sqlString1 = "select _c0 as ID, sum(_c2)/count(_c2) as AverageRating " + \
   "from ratings group by ID order by ID"

users = spark.sql(sqlString1)

users.registerTempTable("users")

sqlString2 = "select first(NumOfUsersWithAnAverageRatingOver3)*100/first(NumOfUsers) as PercentageOfUsersWithAverageRatingOver3 " + \
    "from (select count(ID) as NumOfUsers from users) cross join (select count(ID) as NumOfUsersWithAnAverageRatingOver3 from users where AverageRating >= 3.0)"


res = spark.sql(sqlString2)

res.show()

end = time.time()

print("Time:", (end - start))
