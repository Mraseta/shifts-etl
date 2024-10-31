FROM python:3.8-slim

ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.8.0

RUN pip install "poetry==$POETRY_VERSION"

WORKDIR /api

COPY poetry.lock pyproject.toml /api/

RUN POETRY_VIRTUALENVS_CREATE=false \
    poetry install --no-dev --no-root --no-interaction --no-ansi

COPY ./app /api/app

CMD ["uvicorn", "app.main:app", "--host=0.0.0.0"]
