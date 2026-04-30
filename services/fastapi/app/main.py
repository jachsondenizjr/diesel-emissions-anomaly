from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from app.core.config import settings
from app.routes import emissions, health

app = FastAPI(
    title="Diesel Emissions Anomaly API",
    description="Real-time NOx and PM2.5 anomaly detection for diesel fleet",
    version="1.0.0",
)

Instrumentator().instrument(app).expose(app)

app.include_router(health.router, prefix="/health", tags=["health"])
app.include_router(emissions.router, prefix="/emissions", tags=["emissions"])
