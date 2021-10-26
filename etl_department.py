
from schemas.agent import load_agent
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when, col , udf
from pyspark.sql.types import IntegerType

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
    df_transformed = df.withColumn("created_by", lit(1)) \
                       .withColumn("updated_by", lit(1)) \
                       .drop("id")

    return df_transformed

def load_data(df):
    """Collect data locally and write to CSV.
    :param df: DataFrame to print.
    :return: None
    """
    #write the dataframe into a sql table
    sqlsUrl = 'jdbc:sqlserver://localhost:1433;database=soyspspositivo-2'
    
    df.write.mode("append") \
        .format("jdbc") \
        .option("url", sqlsUrl) \
        .option("dbtable", "Department") \
        .option("user", "sa") \
        .option("password", "Passw1rd") \
        .option('driver', "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
        .save()
    return None

def extract_data(spark):
    """Load data from Parquet file format.
    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    df = spark.read.format('jdbc').options(
        url = "jdbc:postgresql://localhost/jshwelz?user=postgres&password='admin123'",
        database='jshwelz',
        dbtable='public."Department"',
        driver='org.postgresql.Driver',
    ).load()    
    return df
    


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
    