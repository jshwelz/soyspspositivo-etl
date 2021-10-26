
from schemas.agent import load_agent
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import lit, when, col , udf, row_number, current_date
from pyspark.sql.types import IntegerType
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

def run_etl_ticket_images(postgres_url, sql_url):
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
    load_data(sql_url, data_transformed)

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
    
    df_transformed = df.withColumn("created_by", lit(1)) \
                       .withColumn("updated_by", lit(1)) \
                       .drop("url") \
                       .drop("id") 
                       
                                              

    return df_transformed

def load_data(sql_url, df):
    """Collect data locally and write to CSV.
    :param df: DataFrame to print.
    :return: None
    """
    #write the dataframe into a sql table
    sqlsUrl = sql_url
    
    df.write.mode("append") \
        .format("jdbc") \
        .option("url", sqlsUrl) \
        .option("dbtable", "TicketImage") \
        .option("user", Config.SQL_USERNAME) \
        .option("password", Config.SQL_PASSWORD) \
        .option('driver', "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
        .save()
    return None

def extract_data(postgres_url, spark):
    """Load data from Parquet file format.
    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    df = spark.read.format('jdbc').options(
        url = postgres_url,
        database='jshwelz',
        dbtable='public."TicketImage"',
        driver='org.postgresql.Driver',
    ).load()    
    return df
    


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
    