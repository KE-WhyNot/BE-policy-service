from fastapi import APIRouter

router = APIRouter(tags=["Health Check"])

@router.get("/health")
async def health():
    return {"status": "ok", "service": "policy-api"}