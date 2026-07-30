FROM python:3.12-slim

WORKDIR /app/notification_service

COPY notification_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY notification_service/alembic.ini .
COPY notification_service/alembic ./alembic
COPY notification_service/src ./src

ENV PYTHONPATH=/app/notification_service/src
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "notification_service.worker"]
