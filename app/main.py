from fastapi import FastAPI
from app.routers import health
from app.core.cors import setup_cors

# Policy
from app.routers.policy import filter as policy_filter
from app.routers.policy import list as policy_list
from app.routers.policy import id as policy_id

# FinProduct
from app.routers.finproduct import filter as finproduct_filter
from app.routers.finproduct import list as finproduct_list

def create_app() -> FastAPI:
    app = FastAPI(title="Policy Service", version="1.0.0")
    setup_cors(app)
    
    # Health
    app.include_router(health.router, prefix="/api")

    # Policy
    app.include_router(policy_filter.router, prefix="/api/policy")
    app.include_router(policy_list.router, prefix="/api/policy")
    app.include_router(policy_id.router, prefix="/api/policy")

    # FinProduct
    app.include_router(finproduct_filter.router, prefix="/api/finproduct")
    app.include_router(finproduct_list.router, prefix="/api/finproduct")
    
    return app

app = create_app()