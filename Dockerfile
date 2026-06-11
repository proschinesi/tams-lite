FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY vendor/tams-schemas-8.1 ./vendor/tams-schemas-8.1
RUN pip install --no-cache-dir .

ENV TAMS_SCHEMA_DIR=/app/vendor/tams-schemas-8.1
EXPOSE 8000
CMD ["uvicorn", "tamslite.store.app:app", "--host", "0.0.0.0", "--port", "8000"]
