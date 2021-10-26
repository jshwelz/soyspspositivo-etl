
from schemas.agent import load_agent
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def main():
    """Main ETL script definition.
    :return: None
    """
    # start Spark application and get Spark session, logger and config
    scSpark = SparkSession \
        .builder \
        .appName("reading postgresql") \
        .config("spark.jars", "postgresql-42.2.23.jar") \
        .config('spark.driver.extraClassPath', 'sqljdbc/mssql-jdbc-9.4.0.jre8.jar')\
        .config('spark.executor.extraClassPath', 'sqljdbc/mssql-jdbc-9.4.0.jre8.jar')\
        .getOrCreate()

    # log that main ETL job is starting
    # execute ETL pipeline
    data = extract_data(scSpark)
    # data_transformed = transform_data(data, config['steps_per_floor'])
    load_data(data)

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
    df_transformed = (
        df
        .select(
            col('id'),
            concat_ws(
                ' ',
                col('first_name'),
                col('second_name')).alias('name'),
               (col('floor') * lit(steps_per_floor_)).alias('steps_to_desk')))

    return df_transformed

def load_data(df):
    """Collect data locally and write to CSV.
    :param df: DataFrame to print.
    :return: None
    """
    #write the dataframe into a sql table
    sqlsUrl = 'jdbc:sqlserver://localhost:1433;database=soyspspositivo-2'
    df_dropped = df.drop("id", "auth_key", "role_id", "access_token", "logged_in_ip", "logged_in_at", "created_ip")
    # import pdb; pdb.set_trace()    
    df_dropped = df_dropped.withColumnRenamed("status", "is_active")
    df_dropped = df_dropped.withColumn("created_by", lit(1))
    df_dropped = df_dropped.withColumn("first_name", lit(" "))
    df_dropped = df_dropped.withColumn("last_name", lit(" "))
    df_dropped = df_dropped.withColumn("created_by", lit(0))
    df_dropped = df_dropped.withColumn("updated_by", lit(0))
    df_dropped.write.mode("append") \
        .format("jdbc") \
        .option("url", sqlsUrl) \
        .option("dbtable", "[User]") \
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
        dbtable='public."user"',
        driver='org.postgresql.Driver',
    ).load()
    print(type(df))
    return df
    


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()