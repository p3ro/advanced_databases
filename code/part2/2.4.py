from pyspark.sql import SparkSession
import sys, time

disabled = sys.argv[1]
spark = SparkSession.builder.appName('query1-sql').getOrCreate()

if disabled == "Y":
    spark.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.*")

elif disabled == 'N':
  pass
else:
  raise Exception("This setting is not available.")

df = spark.read.format("parquet")

df1 = df.load("hdfs://master:9000/movie_data/ratings.parquet")
df2 = df.load("hdfs://master:9000/movie_data/movie_genres.parquet")

df1.registerTempTable("ratings")
df2.registerTempTable("movie_genres")

sqlString = "SELECT *" + \
" FROM" + \
" (SELECT * FROM movie_genres LIMIT 100) as g," + \
" ratings as r" + \
" WHERE" + \
" r._c1 = g._c0"

t1 = time.time()
spark.sql(sqlString).show()

t2 = time.time()
spark.sql(sqlString).explain()
print("Time with choosing join type% s is% .4f sec."%("enabled" if disabled == 'N' else "disabled", t2-t1))
