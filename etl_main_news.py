from config import Config
from news.etl_news import run_etl_news
from news.etl_news_categories import run_etl_news_categories
from news.etl_news_images import run_etl_news_images

def main(postgres_url, sql_url, sql_url_jdbc):
    run_etl_news_categories(postgres_url, sql_url)
    run_etl_news(postgres_url, sql_url)
    run_etl_news_images(postgres_url, sql_url_jdbc)
    
if __name__ == '__main__':
    postgres_url = "jdbc:postgresql://{host}/{db}?user={user}&password={passwd}".format(user=Config.POSTGRES_USERNAME, passwd=Config.POSTGRES_PASSWORD, 
    host=Config.POSTGRES_HOST, db=Config.POSTGRES_DATABASE)
    
    sql_url = 'mssql+pyodbc://{user}:{passwd}@{host}:{port}/{db}?driver=ODBC+Driver+17+for+SQL+Server'.format(user=Config.SQL_USERNAME, passwd=Config.SQL_PASSWORD, 
    host=Config.SQL_HOST, port=Config.SQL_PORT, db=Config.SQL_DATABASE)
    
    sql_url_jdbc = 'jdbc:sqlserver://{host}:{port};database={db}'.format(host=Config.SQL_HOST, port=Config.SQL_PORT, db=Config.SQL_DATABASE)
    
    main(postgres_url, sql_url, sql_url_jdbc)
    