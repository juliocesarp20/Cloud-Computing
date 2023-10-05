# syntax=docker/dockerfile:1
FROM python:3.9-slim-bullseye
WORKDIR /app
COPY redis_dashboard/requirements.txt .
RUN pip3 install -r requirements.txt
COPY redis_dashboard/ .
CMD [ "python3", "-m" , "app", "run", "--host=0.0.0.0"]
