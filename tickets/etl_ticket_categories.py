
from schemas.agent import load_agent
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import lit, when, col , udf, row_number, current_date
from pyspark.sql.types import IntegerType
import sqlalchemy
import pyodbc
import pandas as pd
from config import Config

def main():
    """Main ETL script definition.
    :return: None
    """
    # start Spark application and get Spark session, logger and config
    scSpark = SparkSession \
        .builder \
        .appName("reading postgresql") \
        .config("spark.jars", "postgresql-42.2.23.jar") \
        .config('spark.driver.extraClassPath', 'mssql-jdbc-9.4.0.jre8.jar')\
        .config('spark.executor.extraClassPath', 'mssql-jdbc-9.4.0.jre8.jar')\
        .getOrCreate()
    
    # log that main ETL job is starting
    # execute ETL pipeline
    data = extract_data(scSpark)
    data_transformed = transform_data(data)
    load_data(data_transformed)

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
    w = Window.orderBy(col('created_by'))
    df_transformed = df.withColumn("created_by", lit(1)) \
                       .withColumn("updated_by", lit(1)) \
                       .withColumn("sla_time_period", lit("hours")) \
                       .withColumn("created_at", current_date()) \
                       .withColumn("language_code", lit("es")) \
                       .withColumn("order",row_number().over(w)) \
                       .drop("name_english") \
                       .drop("agent_id").withColumnRenamed("sla", "sla_time_amount") \
                       

    return df_transformed

def load_data(df, sql_url):
    """Collect data locally and write to CSV.
    :param df: DataFrame to print.
    :return: None
    """
    #write the dataframe into a sql table
    engine = sqlalchemy.create_engine(sql_url)
    with engine.connect() as con:
        con.execute('SET IDENTITY_INSERT TicketCategory ON')
    
    pandasDF = df.toPandas()
    #write the dataframe into a sql table
    pandasDF.to_sql("TicketCategory", engine, if_exists='append', index=False)    

def extract_data(postgres_url, spark):
    """Load data from Parquet file format.
    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    df = spark.read.format('jdbc').options(
        url = postgres_url,
        database='jshwelz',
        dbtable='public."ReportType"',
        driver='org.postgresql.Driver',
    ).load()    
    return df
    
def run_etl_ticket_categories(postgres_url, sql_url):
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
    load_data(data_transformed, sql_url)

    # log the success and terminate Spark application    
    scSpark.stop()
    return None  

# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
    