import pyspark
from pyspark.sql import SparkSession

builder = SparkSession.builder.master("local[*]").appName("RepartitionJoin").getOrCreate()
sc = builder.sparkContext

def processing_map_function(lines, tag):
    
    index = 0
    split_variable = ";"
    
    if tag == "ratings_":
        index = 1
        split_variable = ","
    
    line_splitted = lines.split(split_variable)
    
    return line_splitted[index], tag + lines

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

### Mappers

mapper_source_genres = sc.textFile("hdfs://master:9000/movie_data/100_first_lines_movie_genres.csv").map(lambda x: processing_map_function(x, "genres_"))
mapper_source_ratings = sc.textFile("hdfs://master:9000/movie_data/ratings.csv").map(lambda x: processing_map_function(x, "ratings_"))

### Shuffle and sort

shuffle_phase = mapper_source_genres.join(mapper_source_ratings).reduceByKey(lambda x,y: x + y)

### Reduce Function

reduce_phase = shuffle_phase.map(lambda x: processing_reducer_function(x[0], x[1]))
reduce_phase.saveAsTextFile("output_repartition")
