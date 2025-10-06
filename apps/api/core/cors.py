from fastapi.middleware.cors import CORSMiddleware
from .config import settings

def setup_cors(app):
    origins = settings.cors_origins or ["*"]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )