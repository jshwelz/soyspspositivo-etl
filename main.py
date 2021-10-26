from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
print('Im here 33')

if __name__ == '__main__':
    print('Im here')
    
    scSpark = SparkSession \
        .builder \
        .appName("reading postgresql") \
        .config("spark.jars", "postgresql-42.2.23.jar") \
        .config('spark.driver.extraClassPath', 'mssql-jdbc-9.4.0.jre8.jar')\
        .config('spark.executor.extraClassPath', 'mssql-jdbc-9.4.0.jre8.jar')\
        .getOrCreate()

    dataframe = scSpark.read.format('jdbc').options(
        url = "jdbc:postgresql://localhost/jshwelz?user=postgres&password='admin123'",
        database='jshwelz',
        dbtable='public."Agent"',
        driver='org.postgresql.Driver',
    ).load()

    dataframe.show()

    # sqlsUrl = 'jdbc:sqlserver://docker.for.mac.localhost:1433;database=soyspspositivo-2'

    # qryStr = """ (
    #     SELECT *
    #     FROM Agent
    #     ) t """

    # scSpark.read.format('jdbc')\
    #     .option('url',sqlsUrl)\
    #     .option('driver', "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
    #     .option('dbtable', qryStr )\
    #     .option("user", "sa") \
    #     .option("password", "Passw1rd") \
    #     .load().show()

    