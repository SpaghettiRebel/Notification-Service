from fastapi import FastAPI

from api.routes import router


app = FastAPI(
    title='Public API',
    description='Public HTTP API for Notification Events making',
    version='0.1'
)

app.include_router(router)


@app.get('/health')
async def health_check():
    return {'status': 'ok'}
