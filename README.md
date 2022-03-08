Steps for docker etl
docker build -t soyspspositivo-etl .
docker run -it --rm --name  soyspspositivo-etl-run soyspspositivo-etl
docker exec -it soyspspositivo-etl-run python3 etl_main_news.py




Copy and Run commands inside Docker
docker cp events/etl_events_images.py deede28659ce:/app/events/