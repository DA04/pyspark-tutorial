from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

from os import listdir
from pyspark.sql.functions import col, split, explode, regexp_extract, lower, length

path = './books'
for book in listdir(path):
    df = spark.read.option('encoding', 'UTF-8').text(path+'/'+book)
    lines = df.select(split(col("value"), " ").alias("lines"))
    words = lines.select(explode(col("lines")).alias("words"))
    words_lower = words.select(lower(col("words")).alias("words_lower"))
    words_clean = words_lower.select(regexp_extract(col("words_lower"), "[а-я]*", 0).alias("words_clean"))
    words_clean = words_clean.where(col("words_clean")!="")
    words_clean = words_clean.where(length(col("words_clean"))>2)
    words_moscow = words_clean.where(col("words_clean").contains('вологд'))
    words_moscow.show(20, truncate=50)

# Spark Unstructured Data - Dostoyevski case
# Regex https://regexr.com/3avfl
from os import listdir
from pyspark.sql.functions import col, split, explode, regexp_extract, lower, length

path = './books'
for book in listdir(path):
    df = spark.read.option('encoding', 'UTF-8').text(path+'/'+book)
    lines = df.select(split(col("value"), " ").alias("lines"))
    words = lines.select(explode(col("lines")).alias("words"))
    words = words.select(regexp_extract(col("words"), "[А-Я][а-я]*", 0).alias("words"))
    words = words.where(col("words")!="")
    words = words.where(length(col("words"))>3)
    words = words.groupby("words").count()
    words.orderBy("count", ascending = False).show(20, truncate=50)    