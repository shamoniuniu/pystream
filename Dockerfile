# syntax=docker/dockerfile:1.7
ARG PYTHON_VERSION=3.11.9
ARG PYTHON_BASE_DIGEST=sha256:8fb099199b9f2d70342674bd9dbccd3ed03a258f26bbd1d556822c6dfc60c317

FROM python:${PYTHON_VERSION}-slim-bookworm@${PYTHON_BASE_DIGEST} AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1
WORKDIR /build

COPY pyproject.toml ./
COPY src ./src
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --upgrade pip \
    && /opt/venv/bin/pip install .

FROM python:${PYTHON_VERSION}-slim-bookworm@${PYTHON_BASE_DIGEST} AS runtime

ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYSTREAM_HOME=/opt/pystream

RUN groupadd --gid 10001 pystream \
    && useradd --uid 10001 --gid pystream --create-home --shell /usr/sbin/nologin pystream \
    && mkdir -p /data/artifacts /data/checkpoints /data/output /data/work /opt/pystream \
    && chown -R pystream:pystream /data /opt/pystream

COPY --from=builder /opt/venv /opt/venv
COPY --chown=pystream:pystream examples /opt/pystream/examples
COPY --chown=pystream:pystream scripts /opt/pystream/scripts

USER 10001:10001
WORKDIR /opt/pystream

EXPOSE 8080 8081 9000
STOPSIGNAL SIGTERM

CMD ["python", "-m", "pystream"]
