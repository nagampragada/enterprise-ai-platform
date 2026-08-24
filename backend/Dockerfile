FROM python:3.12-slim-bookworm AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

WORKDIR /build
COPY pyproject.toml ./
COPY src ./src
RUN python -m pip install --upgrade "pip<26" \
    && python -m pip install .

FROM python:3.12-slim-bookworm AS runtime

ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TMPDIR=/tmp

COPY --from=builder /opt/venv /opt/venv
WORKDIR /opt/app
COPY alembic.ini ./alembic.ini
COPY alembic ./alembic

USER 10001:10001
ENTRYPOINT ["python", "-m"]
CMD ["app.server"]
