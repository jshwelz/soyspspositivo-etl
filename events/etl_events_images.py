
from schemas.agent import load_agent
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import lit, when, col , udf, row_number, current_date
from pyspark.sql.types import IntegerType, StringType
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

def run_etl_events_images(postgres_url, sql_url):
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
    map_func_url = udf(lambda row : Config.TICKET_URL + row.split('/')[-1], StringType())
    map_func_path = udf(lambda row : Config.TICKET_PATH + row.split('/')[-1], StringType())
    map_func_cdn = udf(lambda row : row.split('/')[-1], StringType())

    df_transformed = df.withColumn("created_by", lit(1)) \
                       .withColumn("updated_by", lit(1)) \
                       .withColumn("cdn_id", map_func_cdn(df.path)) \
                       .withColumn('file_name',map_func_cdn(df.path)) \
                       .withColumn("url_external_cdn", map_func_cdn(df.path)) \
                       .withColumn("url", map_func_url(df.path)) \
                       .withColumn("path", map_func_path(df.path)) \
                       .withColumn('mime_type',lit('image/jpeg')) \
                       .withColumn('encoding',lit('7bit')) \
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
        .option("dbtable", "EventImage") \
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
        database=Config.POSTGRES_DATABASE,
        dbtable='public."EventImage"',
        driver='org.postgresql.Driver',
    ).load()    
    return df
    


# entry point for PySpark ETL application
if __name__ == '__main__':
    postgres_url = "jdbc:postgresql://{host}/{db}?user={user}&password={passwd}".format(user=Config.POSTGRES_USERNAME, passwd=Config.POSTGRES_PASSWORD, 
    host=Config.POSTGRES_HOST, db=Config.POSTGRES_DATABASE)
    sql_url_jdbc = 'jdbc:sqlserver://{host}:{port};database={db}'.format(host=Config.SQL_HOST, port=Config.SQL_PORT, db=Config.SQL_DATABASE)    
    run_etl_news_images(postgres_url, sql_url_jdbc)
    