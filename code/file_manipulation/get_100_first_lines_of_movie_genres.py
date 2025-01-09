from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("get_100_first_lines").getOrCreate()

df2 = spark.read.csv("hdfs://master:9000/movie_data/movie_genres.csv").limit(100).coalesce(1).write.format("csv").mode("overwrite").option("header", "false").option("inferSchema", "true").save("hdfs://master:9000/movie_data/100_first_lines_movie_genres.csv")

