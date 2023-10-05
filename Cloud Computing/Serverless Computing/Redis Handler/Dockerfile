# syntax=docker/dockerfile:1
FROM python:3.9-slim-bullseye
WORKDIR /app
ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=UTF-8
COPY myhandler/requirements.txt .
RUN pip3 install -r requirements.txt
COPY myhandler/ .
CMD [ "python3", "-m" , "myhandler", "run", "--host=0.0.0.0"]
