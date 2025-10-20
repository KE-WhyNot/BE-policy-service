# 🏛️ BE-Policy-Service

청년정책 및 금융상품 정보를 제공하는 FastAPI 기반 백엔드 서비스입니다.

## 📋 목차

- [프로젝트 개요](#-프로젝트-개요)
- [주요 기능](#-주요-기능)
- [기술 스택](#-기술-스택)
- [프로젝트 구조](#-프로젝트-구조)
- [시작하기](#-시작하기)
- [API 문서](#-api-문서)
- [ELT 파이프라인](#-elt-파이프라인)
- [테스트](#-테스트)
- [배포](#-배포)

## 🎯 프로젝트 개요

WhyNot 프로젝트의 백엔드 정책 서비스로, 청년들에게 유용한 정책 정보와 금융상품 정보를 수집, 가공, 제공합니다.

### 주요 특징

- **RESTful API**: FastAPI를 활용한 고성능 비동기 API
- **ELT 파이프라인**: 외부 데이터 소스로부터 데이터 수집 및 가공
- **필터링 시스템**: 다양한 조건으로 정책 및 금융상품 검색
- **Kubernetes 배포**: 컨테이너화된 배포 및 CronJob 스케줄링
- **PostgreSQL**: 비동기 데이터베이스 연결 지원

## ✨ 주요 기능

### 청년정책 API

- 📋 정책 목록 조회 (필터링 지원)
- 🔍 정책 상세 정보 조회
- 🏷️ 정책 필터 정보 (분야, 지역, 교육수준, 전공, 직업 등)

### 금융상품 API

- 💰 금융상품 목록 조회 (필터링 지원)
- 🏦 금융상품 상세 정보 조회
- 🎁 우대조건 및 은행 필터 정보

### ELT 파이프라인

- 🔄 청년정책 데이터 수집 및 가공
- 💳 금융상품 데이터 수집 및 가공
- 🤖 Gemini API를 활용한 데이터 보강
- ⏰ Kubernetes CronJob 기반 자동화

## 🛠 기술 스택

### Backend Framework

- **FastAPI** (0.117.1) - 고성능 비동기 웹 프레임워크
- **Python** (3.11) - 프로그래밍 언어
- **Uvicorn** - ASGI 서버

### Database

- **PostgreSQL** - 관계형 데이터베이스
- **asyncpg** (0.30.0) - 비동기 PostgreSQL 드라이버
- **SQLAlchemy** - ORM

### External APIs

- **Google Generative AI** (0.8.5) - Gemini API 통합
- **httpx** (0.28.1) - 비동기 HTTP 클라이언트

### DevOps

- **Docker** - 컨테이너화
- **Kubernetes** - 오케스트레이션
- **GitHub Container Registry** - 이미지 저장소

### Testing

- **pytest** - 테스트 프레임워크
- **pytest-asyncio** - 비동기 테스트 지원
- **pytest-cov** - 코드 커버리지

## 📁 프로젝트 구조

```
BE-policy-service/
├── app/
│   ├── main.py                 # FastAPI 애플리케이션 진입점
│   ├── core/                   # 핵심 설정
│   │   ├── config.py          # 환경 변수 설정
│   │   ├── cors.py            # CORS 설정
│   │   └── db.py              # 데이터베이스 연결
│   ├── routers/               # API 라우터
│   │   ├── health.py          # Health Check
│   │   ├── policy/            # 청년정책 API
│   │   │   ├── filter.py      # 필터 조회
│   │   │   ├── list.py        # 목록 조회
│   │   │   └── id.py          # 상세 조회
│   │   └── finproduct/        # 금융상품 API
│   │       ├── filter.py      # 필터 조회
│   │       ├── list.py        # 목록 조회
│   │       └── id.py          # 상세 조회
│   ├── schemas/               # Pydantic 스키마
│   │   ├── policy/            # 정책 스키마
│   │   └── finproduct/        # 금융상품 스키마
│   └── elt/                   # ELT 파이프라인
│       ├── policy/            # 정책 ELT
│       │   ├── 01_raw_ingest.py
│       │   ├── 02_stg_landing.py
│       │   ├── 03_stg_refresh_current.py
│       │   ├── 04_stg_to_core.py
│       │   ├── 05_update_policy_status.py
│       │   └── run_all_policy_elt.py
│       └── finproduct/        # 금융상품 ELT
│           ├── 01_raw_ingest.py
│           ├── 02_stg_base.py
│           ├── 03_stg_option.py
│           ├── 04_stg_to_core.py
│           └── run_all_finproduct_elt.py
├── tests/                     # 테스트 코드
├── Dockerfile                 # Docker 이미지 빌드
├── Makefile                   # 개발 명령어
├── pyproject.toml            # 프로젝트 설정
├── requirements.txt          # Python 의존성
├── job-elt-test.yaml         # Kubernetes Job (정책)
└── job-elt-fin-test.yaml     # Kubernetes Job (금융상품)
```

## 🚀 시작하기

### 사전 요구사항

- Python 3.11+
- PostgreSQL 12+
- (선택) Docker & Kubernetes

### 환경 설정

1. **저장소 클론**

   ```bash
   git clone https://github.com/KE-WhyNot/BE-policy-service.git
   cd BE-policy-service
   ```

2. **가상환경 생성 및 활성화**

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # macOS/Linux
   # .venv\Scripts\activate   # Windows
   ```

3. **의존성 설치**

   ```bash
   make install
   # 또는
   pip install -r requirements.txt
   ```

4. **환경 변수 설정**

   `.env` 파일을 생성하고 다음 내용을 입력하세요:

   ```env
   # 환경
   APP_ENV=dev

   # 데이터베이스
   PG_DSN_ASYNC=postgresql+asyncpg://user:password@localhost:5432/youthpolicy
   PG_DSN_ASYNC_FIN=postgresql+asyncpg://user:password@localhost:5432/finproduct

   # CORS
   CORS_ORIGINS=["http://localhost:3000"]

   # ELT (선택)
   ETL_SOURCE=https://api.example.com
   GEMINI_API_KEY=your-gemini-api-key
   ```

### 실행

#### 개발 서버 실행

```bash
make run
# 또는
uvicorn app.main:app --reload --host 0.0.0.0 --port 8081
```

서버가 실행되면 다음 URL에서 접속 가능합니다:

- API: http://localhost:8081
- Swagger UI: http://localhost:8081/docs
- ReDoc: http://localhost:8081/redoc

## 📚 API 문서

### Health Check

```
GET /health
```

### 청년정책 API

#### 필터 정보 조회

```
GET /api/policy/filter
```

#### 정책 목록 조회

```
GET /api/policy/list?category=일자리&region=서울
```

#### 정책 상세 조회

```
GET /api/policy/{policy_id}
```

### 금융상품 API

#### 필터 정보 조회

```
GET /api/finproduct/filter
```

#### 금융상품 목록 조회

```
GET /api/finproduct/list?bank=KB국민은행
```

#### 금융상품 상세 조회

```
GET /api/finproduct/{product_id}
```

자세한 API 명세는 Swagger UI를 참고하세요: http://localhost:8081/docs

## 🔄 ELT 파이프라인

### 청년정책 ELT

청년정책 데이터를 수집하고 가공하는 파이프라인:

```bash
python app/elt/policy/run_all_policy_elt.py
```

**실행 단계:**

1. `01_raw_ingest.py` - 외부 API에서 원시 데이터 수집
2. `02_stg_landing.py` - 스테이징 테이블에 데이터 적재
3. `03_stg_refresh_current.py` - 현재 데이터 갱신
4. `04_stg_to_core.py` - 스테이징에서 코어 테이블로 변환
5. `05_update_policy_status.py` - 정책 상태 업데이트

### 금융상품 ELT

금융상품 데이터를 수집하고 가공하는 파이프라인:

```bash
python app/elt/finproduct/run_all_finproduct_elt.py
```

**실행 단계:**

1. `01_raw_ingest.py` - 원시 데이터 수집
2. `02_stg_base.py` - 기본 상품 정보 가공
3. `03_stg_option.py` - 옵션 및 우대조건 가공
4. `04_stg_to_core.py` - 코어 테이블로 변환

### Kubernetes CronJob

Kubernetes 환경에서 자동화된 ELT 실행:

```bash
# 청년정책 ELT Job 배포
kubectl apply -f job-elt-test.yaml

# 금융상품 ELT Job 배포
kubectl apply -f job-elt-fin-test.yaml
```

## 🧪 테스트

### 전체 테스트 실행

```bash
make test
```

### 개별 테스트 실행

```bash
# Health Check 테스트
make test-health

# 청년정책 API 테스트
make test-policy

# 금융상품 API 테스트
make test-finproduct

# 통합 테스트
make test-integration
```

### 코드 커버리지

```bash
make test-coverage
```

커버리지 리포트는 `htmlcov/index.html`에서 확인할 수 있습니다.

### 기타 테스트 옵션

```bash
# 상세 출력
make test-verbose

# 빠른 테스트 (DB 없이)
make test-no-db

# Mock API 테스트
make test-mock
```

## 🐳 배포

### Docker 빌드 및 실행

#### 로컬에서 Docker 이미지 빌드

```bash
docker build -t be-policy-service:latest .
```

#### Docker 컨테이너 실행

```bash
docker run -d \
  -p 8082:8082 \
  --env-file .env \
  --name policy-service \
  be-policy-service:latest
```

### Kubernetes 배포

#### 애플리케이션 배포

```bash
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
```

#### ELT CronJob 배포

```bash
# 청년정책 ELT (매일 자정 실행)
kubectl apply -f job-elt-test.yaml

# 금융상품 ELT (매주 월요일 실행)
kubectl apply -f job-elt-fin-test.yaml
```

#### 배포 확인

```bash
# Pod 상태 확인
kubectl get pods -n policy-service

# 로그 확인
kubectl logs -f <pod-name> -n policy-service

# Job 실행 확인
kubectl get jobs -n policy-service
```

### GitHub Container Registry

이미지는 자동으로 빌드되어 GHCR에 푸시됩니다:

```
ghcr.io/ke-whynot/be-policy-service:latest
ghcr.io/ke-whynot/be-policy-service:1.0.0
```

## 📝 개발 가이드

### 코드 스타일

- Python 코드는 PEP 8 스타일 가이드를 따릅니다
- 타입 힌팅을 적극 활용합니다
- Pydantic 모델을 사용하여 데이터 검증을 수행합니다

### 브랜치 전략

- `main`: 프로덕션 코드
- `develop`: 개발 브랜치
- `PRJ-XXX-feat-*`: 기능 개발 브랜치
- `PRJ-XXX-fix-*`: 버그 수정 브랜치

### 커밋 컨벤션

```
feat: 새로운 기능 추가
fix: 버그 수정
docs: 문서 수정
style: 코드 포맷팅
refactor: 코드 리팩토링
test: 테스트 코드
chore: 빌드 업무, 패키지 매니저 설정
```

## 🤝 기여하기

1. Fork the Project
2. Create your Feature Branch (`git checkout -b PRJ-XXX-feat-AmazingFeature`)
3. Commit your Changes (`git commit -m 'feat: Add some AmazingFeature'`)
4. Push to the Branch (`git push origin PRJ-XXX-feat-AmazingFeature`)
5. Open a Pull Request

## 📄 라이선스

이 프로젝트는 KE-WhyNot 팀의 소유입니다.

## 👥 팀

**KE-WhyNot** - WhyNot 프로젝트 팀

## 📧 문의

프로젝트 관련 문의사항은 Issues를 통해 남겨주세요.

---

Made with ❤️ by KE-WhyNot Team
