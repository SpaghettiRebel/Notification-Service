from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, status

from public_api.api.routes import router
from public_api.core.logger import get_logger, setup_logging
from public_api.infrastructure.kafka.producer import KafkaEventPublisher


setup_logging()
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.kafka_publisher = KafkaEventPublisher()
    try:
        yield
    finally:
        await app.state.kafka_publisher.close()


app = FastAPI(
    title='Public API',
    description='Public HTTP API for Notification Events making',
    version='0.1',
    lifespan=lifespan,
)

app.include_router(router, prefix='/message')


@app.get('/health')
async def health_check(request: Request):
    try:
        await request.app.state.kafka_publisher.check_connection()
    except Exception as exc:
        logger.exception("Kafka readiness check failed")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail='Kafka is unavailable',
        ) from exc
    return {'status': 'ok'}


@app.get('/live')
async def liveness_check():
    return {'status': 'ok'}
