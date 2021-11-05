
from schemas.agent import load_agent
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import lit, when, col , udf
from pyspark.sql.types import IntegerType
import sqlalchemy
import pyodbc
import pandas as pd
from config import Config

def main(postgres_url, sql_url):
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


def transform_data(df):
    """Transform original dataset.
    :param df: Input DataFrame.
    :param steps_per_floor_: The number of steps per-floor at 43 Tanner
        Street.
    :return: Transformed DataFrame.
    """
    districts = {"Distrito 1": 1, "Distrito 2" : 2,
                "Distrito 3": 3, "Distrito 4" : 4, "Distrito 5" : 6, "Distrito 7": 6,
                "Distrito 8" : 7, "Distrito 9" : 8 , "Distrito 10" : 9, "Distrito 11" : 10, "Distrito 12" : 11, "Distrito 13" : 12, 
                "Distrito 14" : 13, "Distrito 15" : 14, "Distrito 16" : 15,
	            "Distrito 17" : 16, "Distrito 18" : 17, "Distrito 6" : 19, "Distrito 19" : 21, "Distrito 20" : 22 }
    
    categories = {"Denuncias Ambientales": 1, "Semáforo" : 2,
                "Inundación de Casa por Lluvia": 3, "Toma de Vía Pública" : 4, "Cierre Ilegal de Paso Vehicular" : 6, "Congestionamiento Vial": 6,
                "Colisión Vehicular" : 7, "Deslizamiento" : 8 , "Corte ilegal de árboles" : 9, "Baches" : 10, "Limpieza calle por lluvia" : 11, "Tapa de alcantarilla" : 12, 
                "Tren de Aseo" : 13, "Fuga de agua" : 14, "Solar Baldío" : 15, "Coronavirus" : 16, "Alimentación" : 17}

    
    df_dropped = df.withColumnRenamed("lat", "latitude").withColumnRenamed("lng", "longitude")\
                .withColumn("created_by", lit(1))\
                .withColumn("updated_by", lit(1))\
                .withColumn("district", when(df.district.isNull(), '7').otherwise(df.district))                
    map_func = udf(lambda row : districts.get(row,row))    
    map_func_categ = udf(lambda row : categories.get(row,row))    
    df_transformed = df_dropped.withColumn("district", map_func(col("district")).cast(IntegerType()))\
                            .withColumn("report_id", map_func_categ(col("report_id")).cast(IntegerType()))\
                            .where("status != 'Cerrado'")\
                            .where("name != ''") \
                            .withColumn("status_id", lit(1))\
                            .withColumn("agent_id", lit(1))\
                            .drop("status")\
                            .withColumnRenamed("district", "district_id")\
                            .withColumnRenamed("report_id", "category_id")

    df_filter_date = df_transformed.filter(df_transformed.created_at >= lit(Config.TICKETS_DATE))
    return df_filter_date

def load_data(sql_url, df):
    """Collect data locally and write to SQL.
    :param df: DataFrame to print.
    :return: None
    """
    
    # establishing the connection to the database using engine as an interface
    engine = sqlalchemy.create_engine(sql_url)
    with engine.connect() as con:
        con.execute('SET IDENTITY_INSERT Ticket ON')
    
    pandasDF = df.toPandas()
    #write the dataframe into a sql table
    pandasDF.to_sql("Ticket", engine, if_exists='append', index=False)    
 
    return None

def extract_data(postgres_url, spark):
    """Load data from Parquet file format.
    :param spark: Spark session object.
    :return: Spark DataFrame.
    """    
    df = spark.read.format('jdbc').options(
        url = postgres_url,
        database='jshwelz',
        dbtable='(select A.* from public."Ticket" A  where A.status != \'Cerrado\') as Ticket',
        driver='org.postgresql.Driver',
    ).load()
    print(type(df))            
    return df
    
def run_etl_ticket(postgres_url, sql_url):
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
    main(postgres_url, sql_url)
    