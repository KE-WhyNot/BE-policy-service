import pytest
from datetime import date, timedelta
from fastapi.testclient import TestClient

from app.main import create_app
from app.core.db import get_fin_db, get_db


class MockMappings:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class MockResult:
    def __init__(self, *, scalar=None, rows=None):
        self._scalar = scalar
        self._rows = rows or []

    def scalar(self):
        return self._scalar

    def mappings(self):
        return MockMappings(self._rows)


class MockFinProductSession:
    def __init__(self):
        self._rows = [
            {
                "id": 101,
                "bank_id": 1,
                "bank_name": "모크은행",
                "bank_image_url": "https://example.com/mock-bank-logo.png",
                "product_name": "모크 자유적금",
                "join_member": "누구나 가입가능",
                "etc_note": None,
                "join_ways": ["인터넷"],
                "max_interest_rate": 5.5,
                "min_interest_rate": 2.0,
                "is_non_face_to_face": True,
                "is_bank_app": True,
                "is_salary_linked": False,
                "is_utility_linked": False,
                "is_card_usage": False,
                "is_first_transaction": False,
                "is_checking_account": False,
                "is_pension_linked": False,
                "is_redeposit": False,
                "is_subscription_linked": False,
                "is_recommend_coupon": False,
                "is_auto_transfer": False,
            }
        ]

    async def execute(self, query, params=None):
        query_text = str(query)
        if "SELECT COUNT" in query_text.upper():
            return MockResult(scalar=len(self._rows))
        return MockResult(rows=self._rows)


class MockPolicySession:
    def __init__(self):
        closing_date = date.today() + timedelta(days=10)
        self._rows = [
            {
                "id": "501",
                "status": "OPEN",
                "apply_type": "PERIODIC",
                "apply_end": closing_date,
                "category_large": "청년지원",
                "title": "모크 청년 도약 자금",
                "summary_raw": "청년 대상 생활자금 지원 프로그램",
                "period_apply": "2024-01-01 ~ 2024-12-31",
                "keyword": "청년, 생활, 지원",
            }
        ]

    async def execute(self, query, params=None):
        query_text = str(query)
        if "SELECT COUNT" in query_text.upper():
            return MockResult(scalar=len(self._rows))
        return MockResult(rows=self._rows)


def test_finproduct_list_with_mock_data():
    app = create_app()

    async def override_fin_db():
        yield MockFinProductSession()

    app.dependency_overrides[get_fin_db] = override_fin_db

    with TestClient(app) as client:
        response = client.get("/api/finproduct/list")

    assert response.status_code == 200
    payload = response.json()
    result = payload["result"]

    assert result["pagging"]["total_count"] == 1
    assert len(result["finProductList"]) == 1
    product = result["finProductList"][0]

    assert product["product_name"] == "모크 자유적금"
    assert product["bank_name"] == "모크은행"
    assert "누구나 가입" in product["product_type_chip"]

    app.dependency_overrides.clear()


def test_policy_list_with_mock_data():
    app = create_app()

    async def override_policy_db():
        yield MockPolicySession()

    app.dependency_overrides[get_db] = override_policy_db

    with TestClient(app) as client:
        response = client.get("/api/policy/list")

    assert response.status_code == 200
    payload = response.json()
    result = payload["result"]

    assert result["pagging"]["total_count"] == 1
    assert len(result["youthPolicyList"]) == 1
    policy = result["youthPolicyList"][0]

    assert policy["policy_id"] == "501"
    assert policy["title"] == "모크 청년 도약 자금"
    assert policy["status"].startswith("마감 D-")
    assert "청년" in policy["keyword"]

    app.dependency_overrides.clear()
