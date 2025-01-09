from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("csv_to_parquet").getOrCreate()

df1 = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("hdfs://master:9000/movie_data/movies.csv")
df1.write.mode('overwrite').parquet("hdfs://master:9000/movie_data/movies.parquet")
df2 = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("hdfs://master:9000/movie_data/movie_genres.csv")
df2.write.mode('overwrite').parquet("hdfs://master:9000/movie_data/movie_genres.parquet")
df3 = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("hdfs://master:9000/movie_data/ratings.csv")
df3.write.mode('overwrite').parquet("hdfs://master:9000/movie_data/ratings.parquet")
