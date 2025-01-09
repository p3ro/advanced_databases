import pyspark
from pyspark.sql import SparkSession

builder = SparkSession.builder.appName("BroadcastJoin").getOrCreate()
sc = builder.sparkContext

def processing_mapper_function(data_value, cache_table):
    
    values = data_value.split(",")
    genres = cache_table
    lines = ""
    
    genres.append(data_value)
    for x in genres:
        lines += x
        
    return values[1], genres

def processing_reducer_function(key, data_value):
        
    genre_titles = ""
    ratings = ""
    
    for elem in data_value:
        
        split_value = ","
        is_genre = False
        
        if str(elem).startswith("genres"):
    
            is_genre = True
        
        elem_splitted = elem.split(split_value)
        
        if is_genre:
            genre_titles += "{0}".format(("," if len(genre_titles) > 0 else "") + elem_splitted[1])
        else:
            ratings += "{0}".format(("," if len(ratings) > 0 else "") + elem_splitted[2])
                
    return "Movie ID {0}: Genres: [{1}] Ratings: [{2}]".format(key, genre_titles, ratings)

## Mapper Read Small Table

table = mapper_small_table = sc.textFile("hdfs://master:9000/movie_data/100_first_lines_movie_genres.csv").cache().collect()

## Mapper Join Phase

mapper_join_phase = sc.textFile("hdfs://master:9000/movie_data/ratings.csv").map(lambda x: processing_mapper_function(x, table))

### Shuffle and sort

shuffle_phase = mapper_join_phase.reduceByKey(lambda x,y: x + y)

### Reduce Function

reduce_phase = shuffle_phase.map(lambda x: processing_reducer_function(x[0], x[1]))
reduce_phase.saveAsTextFile("output_broadcasting")
