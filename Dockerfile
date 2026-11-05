FROM python:3.12-slim@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1 AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY job.py .
COPY scripts/findings.py scripts/generate_data.py scripts/

RUN useradd --create-home --uid 10001 etl
USER etl

ENTRYPOINT ["python", "job.py"]

# Dashboard + notebook builder. Writes to a bind-mounted site/ directory.
FROM runtime AS reports

USER root
COPY requirements-reports.txt .
RUN pip install -r requirements-reports.txt
COPY scripts/ scripts/
COPY notebooks/ notebooks/
ENV MPLCONFIGDIR=/tmp/matplotlib
USER etl
ENTRYPOINT ["bash", "scripts/build_site.sh"]
