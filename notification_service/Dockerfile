FROM python:3.12-slim

WORKDIR /app/public_api

COPY notification_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY notification_service/src ./src

ENV PYTHONPATH=/app/notification_service/src

CMD ["python", "-m", "notification_service.worker"]