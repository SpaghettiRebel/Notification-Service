from fastapi import FastAPI

from public_api.api.routes import router
from public_api.core.logger import setup_logging


setup_logging()

app = FastAPI(
    title='Public API',
    description='Public HTTP API for Notification Events making',
    version='0.1'
)

app.include_router(router, prefix='/message')


@app.get('/health')
async def health_check():
    return {'status': 'ok'}
