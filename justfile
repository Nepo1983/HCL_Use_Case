up:
    docker-compose build && docker-compose up airflow-init && docker-compose up
down:
    docker compose down --volumes --rmi all
