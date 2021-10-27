
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


def transform_data(df):
    """Transform original dataset.
    :param df: Input DataFrame.
    :param steps_per_floor_: The number of steps per-floor at 43 Tanner
        Street.
    :return: Transformed DataFrame.
    """    
    
    df_transformed = df.withColumn("created_by", lit(1)) \
                       .withColumn("updated_by", lit(1)) \
                       .withColumnRenamed("note", "comment")\
                       .drop("id") 
                       
                                              

    return df_transformed

def load_data(sql_url, df):
    """Collect data locally and write to SQL.
    :param df: DataFrame to print.
    :return: None
    """
    #write the dataframe into a sql table
    # sqlsUrl = 'jdbc:sqlserver://localhost:1433;database=soyspspositivo-2'        
    df.write.mode("append") \
        .format("jdbc") \
        .option("url", sql_url) \
        .option("dbtable", "TicketComment") \
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
        dbtable='(select A.* from public."TicketComment" A inner join public."Ticket" B on A.ticket_id = B.id where B.status != \'Cerrado\') as TicketComment',
        driver='org.postgresql.Driver',
    ).load()    
       
    return df
    
def run_etl_ticket_comments(postgres_url, sql_url):
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

# entry point for PySpark ETL application
if __name__ == '__main__':
    postgres_url = "jdbc:postgresql://{host}/{db}?user={user}&password={passwd}".format(user=Config.POSTGRES_USERNAME, passwd=Config.POSTGRES_PASSWORD, 
    host=Config.POSTGRES_HOST, db=Config.POSTGRES_DATABASE)
    
    sql_url = 'mssql+pyodbc://{user}:{passwd}@{host}:{port}/{db}?driver=ODBC+Driver+17+for+SQL+Server'.format(user=Config.SQL_USERNAME, passwd=Config.SQL_PASSWORD, 
    host=Config.SQL_HOST, port=Config.SQL_PORT, db=Config.SQL_DATABASE)
    run_etl_ticket_comments(postgres_url, sql_url)
    
    