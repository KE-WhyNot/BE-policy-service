from fastapi import FastAPI
from app.routers import health
from app.core.cors import setup_cors
from app.routers.policy import filter, id, list

def create_app() -> FastAPI:
    app = FastAPI(title="Policy Service", version="1.0.0")
    setup_cors(app)
    
    # Health
    app.include_router(health.router, prefix="/api")

    # Policy
    app.include_router(filter.router, prefix="/api/policy")
    app.include_router(list.router, prefix="/api/policy")
    app.include_router(id.router, prefix="/api/policy")

    # FinProduct

    return app

app = create_app()