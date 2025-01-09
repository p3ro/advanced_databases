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

movies = spark.read.format('csv').\
    options(header = 'false', inferSchema = 'true').\
    load("hdfs://master:9000/movie_data/movies.csv")

movies.registerTempTable("movies")

sqlString1 = "select m._c0 as MovieID, m._c1 as Title, g._c1 as Genre, m._c7 as Popularity, r._c0 as UserID, r._c2 as Rating " + \
    "from movies as m, genres as g, ratings as r where m._c0 = g._c0 and m._c0 = r._c1"

analytical_reviews = spark.sql(sqlString1)

analytical_reviews.registerTempTable("analytical_reviews")

sqlString2 = "select Genre, UserID, count(MovieID) as NumberOfRatings from analytical_reviews group by Genre, UserID"

num_of_ratings = spark.sql(sqlString2)

num_of_ratings.registerTempTable("num_of_ratings")

sqlString3 = "select n.Genre, n.UserID, r.MostRatings as MostRatingsInGenre from num_of_ratings as n, (select Genre, max(NumberOfRatings) as MostRatings from num_of_ratings group by Genre) as r " + \
    "where n.Genre = r.Genre and r.MostRatings = n.NumberOfRatings"

most_ratings = spark.sql(sqlString3)

most_ratings.registerTempTable("most_ratings")

sqlString4 = "select m.Genre as Genre, max(a.Rating) as MaxRating, min(a.Rating) as MinRating " + \
    "from most_ratings as m, analytical_reviews as a where m.UserID = a.UserID and m.Genre = a.Genre group by m.Genre"

max_min_ratings = spark.sql(sqlString4)

max_min_ratings.registerTempTable("max_min_ratings")

sqlString5 = "select m.Genre as Genre, a.UserID as UserID, m.MaxRating as MaxRating, max(a.Popularity) as MaxPopularity " + \
    "from max_min_ratings as m, analytical_reviews as a where m.Genre = a.Genre and m.MaxRating = a.Rating group by m.Genre, a.UserID, m.MaxRating" 

max_rating_popularity = spark.sql(sqlString5)

max_rating_popularity.registerTempTable("max_rating_popularity")

sqlString6 = "select m.Genre as Genre, a.UserID as UserID, m.MinRating as MinRating, max(a.Popularity) as MaxPopularity " + \
    "from max_min_ratings as m, analytical_reviews as a where m.Genre = a.Genre and m.MinRating = a.Rating group by m.Genre, a.UserID, m.MinRating"

min_rating_popularity = spark.sql(sqlString6)

min_rating_popularity.registerTempTable("min_rating_popularity")

sqlString7 = "select r.Genre, r.UserID, r.MostRatingsInGenre, a.Title as BestMovie, m.MaxRating " + \
    "from max_rating_popularity as m, most_ratings as r, analytical_reviews as a " + \
    "where m.Genre = a.Genre and m.Genre = r.Genre and m.UserID = a.UserID and m.UserID = r.UserID and m.MaxRating = a.Rating and m.MaxPopularity = a.Popularity"

best_movies = spark.sql(sqlString7)

best_movies.registerTempTable("best_movies")

sqlString8 = "select r.Genre, r.UserID, r.MostRatingsInGenre, a.Title as WorstMovie, m.MinRating " + \
    "from min_rating_popularity as m, most_ratings as r, analytical_reviews as a " + \
    "where m.Genre = a.Genre and m.Genre = r.Genre and m.UserID = a.UserID and m.UserID = r.UserID and m.MinRating = a.Rating and m.MaxPopularity = a.Popularity"

worst_movies = spark.sql(sqlString8)

worst_movies.registerTempTable("worst_movies")

sqlString9 = "select b.Genre, b.UserID as GenreProUser, b.MostRatingsInGenre as RatingsInGenre, b.BestMovie, b.MaxRating as BestMovieRating, w.WorstMovie, w.MinRating as WorstMovieRating " + \
    "from best_movies as b, worst_movies as w where b.Genre = w.Genre order by b.Genre"

res = spark.sql(sqlString9)

res.show()

end = time.time()

print("Time:", (end - start))
