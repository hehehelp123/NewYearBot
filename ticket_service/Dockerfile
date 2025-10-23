FROM python:3.12-slim as builder

WORKDIR /app

ENV POETRY_HOME="/opt/poetry"
ENV POETRY_VIRTUALENVS_IN_PROJECT=true
ENV PATH="$POETRY_HOME/bin:$PATH"

RUN apt-get update && apt-get install --no-install-recommends -y build-essential \
    && pip install --no-cache-dir poetry \
    && apt-get purge -y build-essential && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml poetry.lock ./
RUN poetry install --no-root

FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install --no-install-recommends -y curl && rm -rf /var/lib/apt/lists/*

ENV POETRY_HOME="/opt/poetry"
ENV POETRY_VIRTUALENVS_IN_PROJECT=true
ENV PATH="$POETRY_HOME/bin:$PATH"

COPY --from=builder /opt/poetry /opt/poetry
COPY --from=builder /app/.venv ./.venv

COPY . .

EXPOSE 8004