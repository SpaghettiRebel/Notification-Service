FROM python:3.12-slim

WORKDIR /app/public_api

COPY public_api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY public_api/src ./src

ENV PYTHONPATH=/app/public_api/src
ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "public_api.main:app", "--host", "0.0.0.0", "--port", "8001"]
