from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os, sys

# По умолчанию считаем что csv-файл находится в одной папке со скриптом
folder = os.getcwd()

# Аргумент коммандной строки - название csv-файла
# Для запуска скрипта нужно набрать например: python3 test.py test.csv 
csv_source = sys.argv[1]

# Количество записей в месяце для пункта 2 задания
number_of_records = 3

# Диапазон городов для пункта 4 задания
list_of_cities = ['Rome', 'Kiev']

# Создание соединения
sc = SparkContext('local[*]')

# Возврат сессии или создание новой
spark = SparkSession.builder.getOrCreate()

# Задание первоначальной схемы DF
cityColumn = StructField("city",StringType(),True)
datestrColumn = StructField("datestr",StringType(),True)
temperatureColumn = StructField("temperature",DecimalType(3,1),True)

columnList = [cityColumn, datestrColumn, temperatureColumn]
citiesDFShema = StructType(columnList)

# Загрузка таблицы из csv-файла
cities = spark.read.csv(folder+'/'+csv_source, header=False, schema=citiesDFShema)

# Преобразование столбца дат
cities = cities.select('city', to_date(cities.datestr, 'yyyy-MM-dd').alias('date'), 'temperature')

# Добавление столбца month
cities2 = cities.withColumn('month', month('date'))

# Создание DF со агрегатными значениями температур, переименование 
# столбцов, добавление id
cities_max = cities2.groupBy('city','month').max('temperature').withColumnRenamed('max(temperature)','max').withColumn('id',monotonically_increasing_id())
cities_min = cities2.groupBy('city','month').min('temperature').withColumnRenamed('min(temperature)','min').withColumn('id_min',monotonically_increasing_id()).drop('city','month')
cities_avg = cities2.groupBy('city','month').avg('temperature').withColumnRenamed('avg(temperature)','avg').withColumn('id_avg',monotonically_increasing_id()).drop('city','month')
cities_count = cities2.groupBy('city', 'month').count().withColumnRenamed('count','number').withColumn('id_count',monotonically_increasing_id()).drop('city','month')

# Обьединение созданных DF в один
response = cities_max.join(cities_min,(cities_max.id==cities_min.id_min),how="inner")
response = response.join(cities_avg,response.id==cities_avg.id_avg,how="inner")
response = response.join(cities_count,response.id==cities_count.id_count,how="inner").drop('id','id_min','id_avg','id_count')
response = response.orderBy('city','month')

# Ответ на 1-ый вопрос ТЗ
response1 = response.drop('number')
response1.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("1")

# Ответ на 2-ой вопрос ТЗ
response2 = response.filter(response.number>=number_of_records).drop('number')
response2.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("2")

# Вспомогательная таблица с city, month, avg и number
helper = response.drop('id').withColumnRenamed('city','c').withColumnRenamed('month','m')

# Ответ на 3-ий вопрос
response3 = cities2.join(helper,(cities2.city==helper.c)&(cities2.month==helper.m),how='left')
response3 = response3.withColumn('diff_max',response3.temperature-response3.max).drop('max')
response3 = response3.withColumn('diff_min',response3.temperature-response3.min).drop('min')
response3 = response3.withColumn('diff_avg',response3.temperature-response3.avg).drop('avg')
response3 = response3.drop('month','number','temperature','c','m')
response3.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("3")

# Ответ на 4-ый вопрос
response4=response1.filter(response1.city.isin(list_of_cities))
response4.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("4")
