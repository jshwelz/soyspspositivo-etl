FROM jupyter/pyspark-notebook
WORKDIR /app
USER root
COPY . .
RUN apt-get update 
RUN apt-get -y install gcc g++ vim gnupg2 curl unixodbc-dev unixodbc
RUN chmod +x sql.sh
RUN ./sql.sh
RUN pip3 install -r requirements.txt
CMD [ "python3", "etl_main_tickets.py" ]
