# Spark Delimited data case
# https://rosnedra.gov.ru/opendata
import pyspark.sql.functions as F
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import DateType
from pyspark.sql.functions import *

dir = './tables'
logs = spark.read.csv(os.path.join(dir, './tables/data-68-structure-2.csv'), sep=';', header=True, inferSchema=True)
logs = logs.withColumn('Дата регистрации', regexp_replace('Дата регистрации', '[.]', '-'))
logs = logs.withColumn('timestamp_converted', to_date(F.col('Дата регистрации'),"dd-MM-yyyy"))
logs.printSchema()
logs.select(*['Дата регистрации', 'timestamp_converted']).show(5)
print(logs.count())