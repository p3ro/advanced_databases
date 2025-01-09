from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys, time
disabled ="Y"
disabled = sys.argv [1]
spark = SparkSession.builder.appName('CompareJoins').getOrCreate()
if disabled == "Y":
  spark.conf.set("spark.executor.memory", "10g")
  spark.conf.set("spark.executor.cores", "2")
elif disabled == 'N':
  pass
else:
  pass
#raise Exception ("This setting is not available.")
#df = spark.read.format ("parquet")
df1 = spark.read.option("header","false").option("delimiter",",").option("inferschmea","true").csv("/FileStore/tables/ratings.csv")
df2=spark.read.option("header","false").option("delimiter",",").option("inferSchema","true").csv("/FileStore/tables/movie_genres.csv")

sqlString = \
"SELECT * " + \
"FROM" + \
" (SELECT * FROM movie_genres LIMIT 100) as g," + \
" ratings as r" + \
" WHERE" + \
" r._c1 = g._c0"
#91500
threshold_limit=500

def Broadcast():
  if(df1.count()<=threshold_limit):
    flag=1
  elif(df2.count()<=threshold_limit):
    flag=2
  else:
    return False
  print(flag)
  if(flag==1):
    df1_bd = broadcast(df1)
    df2_bd=df2
  elif(flag==2):
    df1_bd=df1
    df2_bd = broadcast(df2)
  df1_bd.registerTempTable("ratings")
  df2_bd.registerTempTable("movie_genres")
  
  t1 = time.time ()
  spark.sql(sqlString).show()
  t2 =time.time()
  
  spark.sql(sqlString).explain()
  print ("Time with choosing join type% s is% .4f sec."% ("enabled" if
  disabled == 'N' else "disabled", t2-t1))
  return True
  
def Repartition():   
  df1_bd=df1.repartition('_c1')
  df2_bd=df2.repartition('_c0')
  
  df1_bd.registerTempTable("ratings")
  df2_bd.registerTempTable("movie_genres")
  
  t1 = time.time ()
  spark.sql(sqlString).show()
  t2 =time.time()
  
  spark.sql(sqlString).explain()
  print ("Time with choosing join type% s is% .4f sec."% ("enabled" if
  disabled == 'N' else "disabled", t2-t1))

b_obj=Broadcast()
if(b_obj==False):
  r_obj=Repartition()

