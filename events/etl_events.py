
from schemas.agent import load_agent
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when, col , udf
from pyspark.sql.types import IntegerType, StringType
from config import Config
import sqlalchemy
import uuid


def clean_events(sql_url):
    engine = sqlalchemy.create_engine(sql_url)
    with engine.connect() as con:
        con.execute('delete from EventImage')
        con.execute('delete from EventCategory')
        con.execute('delete from Event')


def run_etl_events(postgres_url, sql_url_jdbc):
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
    mapping = {False: 3, True : 2}
    map_func = udf(lambda row : mapping.get(row,row))  
    uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())
    df_transformed = df.withColumn("created_by", lit(1)) \
                       .withColumn("updated_by", lit(1)) \
                       .withColumn("slug",uuidUdf())\
                       .withColumn("excerpt", df.name)\
                       .withColumn("language_code", lit('es')) \
                       .withColumnRenamed("start", "start_date")\
                       .withColumnRenamed("end", "end_date")\
                       .withColumn("published_at", df.created_at) \
                       .withColumn("status_id", lit(7))\
                       .drop("description_english_text")\
                       .drop("main_image")\
                       .withColumnRenamed("category_id", "event_category_id")\
                       .withColumnRenamed("lat", "latitude")\
                       .withColumnRenamed("lng", "longitude")\
                       .drop("price")\
                       .drop("name_english")                                              

    return df_transformed

def load_data(sql_url, df):
    """Collect data locally.
    :param df: DataFrame to print.
    :return: None
    """
    #write the dataframe into a sql table
        
    # establishing the connection to the database using engine as an interface
    engine = sqlalchemy.create_engine(sql_url)
    with engine.connect() as con:
        con.execute('SET IDENTITY_INSERT Event ON')
    
    pandasDF = df.toPandas()
    #write the dataframe into a sql table
    pandasDF.to_sql("Event", engine, if_exists='append', index=False)    

def extract_data(postgres_url, spark):
    """Load data from Parquet file format.
    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    df = spark.read.format('jdbc').options(
        url = postgres_url,
        database=Config.POSTGRES_DATABASE,
        dbtable='public."Event"',
        driver='org.postgresql.Driver',
    ).load()    
    return df
    


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
    