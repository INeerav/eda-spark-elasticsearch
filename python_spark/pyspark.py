# -*- coding: utf-8 -*-


!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q http://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz
!tar xf spark-3.1.1-bin-hadoop3.2.tgz
!pip install -q findspark

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.1.1-bin-hadoop3.2"

!ls

import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True) # Property used to format output tables better
spark

# Load data from csv to a dataframe.
# header=True means the first row is a header
# sep=';' means the column are seperated using ''
df_artists = spark.read.csv('artists.csv', header=True, sep=",")
df_artists.show(5)
df_artists.count()

#df_artists.columns;
df_artists.printSchema()

# cleaning and transforming
# removing duplicate values first
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import isnan, when, count, col, lit, trim, avg, ceil
from pyspark.sql.types import StringType
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

df_artists = df_artists.dropDuplicates(['mbid'])
print(df_artists.count())

# string cleaning and removing unwanted
str_cols = [item[0] for item in df_artists.dtypes if item[1].startswith('string')]
for cols in str_cols:
  df_artists = df_artists.withColumn(cols, trim(df_artists[cols]))

# lowercasing the columns and put the underscore

# create the temp view table for the sql operations
df_artists.createOrReplaceTempView("artisttbl")

# to understand the data
# define the categories
# 1. artist_mb
# 2. artist_lastfm
# 3. artist_lastf
# 4. country_mb
# 5. country_lastfm
# 6. tags_mb
# 7. ambiguous_artist bool check True or false

# * indicator_code : to represent the valuation indicator code unit name

query_country_counts = '''select row_number() over (order by country_mb asc) rn, country_mb country, count(*) count
from artisttbl art
where art.country_mb is not null
group by country_mb having count(*) > 1'''

query_artist_counts = '''select row_number() over (order by artist_mb asc) rn, country_mb country, artist_mb artist, count(*) count
from artisttbl art
where art.artist_mb is not null and art.country_mb is not null
group by artist_mb,country_mb having count(*) > 1'''

query_clean_known_artists = '''select * from artisttbl art where
ambiguous_artist = FALSE
and tags_mb is not null
and tags_lastfm is not null
and country_mb is not null
and country_lastfm is not null
 '''
clean_all_artists = '''select * from artisttbl art where
 tags_mb is not null
and tags_lastfm is not null
and country_mb is not null
and country_lastfm is not null
 '''
# 36898
# df_known_artists = spark.sql(clean_all_artists)
df_all_artists = spark.sql(clean_all_artists)
df_all_artists.count()

df_all = df_all_artists.toPandas()
df_all.to_csv('all_artists.csv', index = False, encoding='utf-8')

df_known_artists = df_known_artists.toPandas()
df_all.to_csv('known_artists.csv', index = False, encoding='utf-8')

df_all_artists

# transform RDDs