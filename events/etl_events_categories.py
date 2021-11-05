
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when, col, udf, trim
from pyspark.sql.types import IntegerType
import sqlalchemy
import pyodbc
import pandas as pd
from config import Config

def run_etl_events_categories(postgres_url, sql_url_jdbc):
    """Main ETL script definition.
    :return: None
    """
    # start Spark application and get Spark session, logger and config
    scSpark = SparkSession \
        .builder \
        .appName("reading postgresql") \
        .config("spark.jars", "drivers/postgresql-42.2.23.jar") \
        .config('spark.driver.extraClassPath', 'drivers/mssql-jdbc-9.4.0.jre8.jar')\
        .config('spark.executor.extraClassPath', 'drivers/mssql-jdbc-9.4.0.jre8.jar')\
        .getOrCreate()
    
    # log that main ETL job is starting
    # execute ETL pipeline
    data = extract_data(postgres_url, scSpark)
    data_transformed = transform_data(data)
    load_data(sql_url_jdbc, data_transformed)

    # log the success and terminate Spark application    
    scSpark.stop()
    return None    


def transform_data(df):
    """Transform original dataset.
    :param df: Input DataFrame.
    :param steps_per_floor_: The number of steps per-floor at 43 Tanner
        Street.
    :return: Transformed DataFrame.
    """    
    df_transformed = df.withColumn("name", trim("name"))
    df_transformed = df_transformed.withColumn("created_by", lit(1)) \
                       .withColumn("updated_by", lit(1)) \
                       .withColumn("language_code", lit('es')) \
                       .filter(df.name != "" ) \
                       .dropDuplicates(['name']) \
                       .drop("name_english")

    
    return df_transformed

def load_data(sql_url, df):
    """Collect data locally and write to CSV.
    :param df: DataFrame to print.
    :return: None
    """
    #write the dataframe into a sql table
    #write the dataframe into a sql table
    engine = sqlalchemy.create_engine(sql_url)
    with engine.connect() as con:
        con.execute('SET IDENTITY_INSERT EventCategory ON')
    
    pandasDF = df.toPandas()
    #write the dataframe into a sql table
    pandasDF.to_sql("EventCategory", engine, if_exists='append', index=False)         


def extract_data(postgres_url, spark):
    """Load data from Parquet file format.
    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    df = spark.read.format('jdbc').options(
        url = postgres_url,
        database=Config.POSTGRES_DATABASE,
        dbtable='public."EventCategory"',
        driver='org.postgresql.Driver',
    ).load()    
    return df
    


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
    