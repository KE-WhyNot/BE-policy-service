from fastapi import APIRouter

router = APIRouter(tags=["policy"])

@router.get(
    "/policy",
    response_model = PolicyListResponse,
    responses = {
        200: {"description": "정책 리스트 조회 성공"},
        400: {"description": "잘못된 요청"},
        500: {"description": "서버 오류"},
    }
)