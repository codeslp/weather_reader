FROM python:3.10-slim

WORKDIR /app

COPY poetry.lock pyproject.toml /app/

RUN apt-get update && \
    apt-get install -y libpq-dev gcc

RUN pip install poetry && \
    poetry config virtualenvs.create false && \
    poetry install --no-dev --no-interaction --no-ansi

COPY . /app