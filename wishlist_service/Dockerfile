FROM python:3.12-slim as builder

WORKDIR /app

ENV POETRY_HOME="/opt/poetry"
ENV POETRY_VIRTUALENVS_IN_PROJECT=true
ENV PATH="$POETRY_HOME/bin:$PATH"

RUN apt-get update && apt-get install --no-install-recommends -y curl build-essential
RUN curl -sSL https://install.python-poetry.org | python -

COPY pyproject.toml poetry.lock ./
RUN poetry install --no-root

FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install --no-install-recommends -y curl && rm -rf /var/lib/apt/lists/*

COPY --from=confluentinc/cp-kafka:7.5.3 /usr/bin/kafka-topics /usr/bin/kafka-topics
COPY wait-for-kafka.sh /usr/bin/wait-for-kafka.sh
RUN chmod +x /usr/bin/wait-for-kafka.sh

ENV POETRY_HOME="/opt/poetry"
ENV POETRY_VIRTUALENVS_IN_PROJECT=true
ENV PATH="$POETRY_HOME/bin:$PATH"

COPY --from=builder /opt/poetry /opt/poetry
COPY --from=builder /app/.venv ./.venv

COPY . .

EXPOSE 8003