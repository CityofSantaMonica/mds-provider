FROM python:3

WORKDIR /usr/src/mds

COPY . .

RUN apt-get update -y && \
    pip install --upgrade pip && \
    pip install pipenv && \
    pipenv update --dev
