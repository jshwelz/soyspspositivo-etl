from pyspark.sql import SparkSession
from pyspark.sql import SQLContext





if __name__ == '__main__':
    print('Im here')
    scSpark = SparkSession \
        .builder \
        .appName("reading csv") \
        .config("spark.jars", "postgresql-42.2.23.jar") \
        .getOrCreate()

    dataframe = scSpark.read.format('jdbc').options(
        url = "jdbc:postgresql://localhost/jshwelz?user=postgres&password='admin123'",
        database='jshwelz',
        dbtable='public."Agent"',
        driver='org.postgresql.Driver',
    ).load()

    dataframe.show()