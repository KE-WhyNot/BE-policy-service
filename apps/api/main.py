from fastapi import FastAPI
from apps.api.core.cors import setup_cors
from apps.api.routers import health, master

def create_app() -> FastAPI:
    app = FastAPI(title="Policy Service", version="1.0.0")
    setup_cors(app)
    app.include_router(health.router, prefix="/api")
    app.include_router(master.router,  prefix="/api")
    return app

app = create_app()