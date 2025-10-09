# Python 3.11 슬림 이미지 사용
FROM python:3.11-slim

# 환경 변수 (포트와 파이썬 / pip 동작 관련)
ENV PORT=8082 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# 작업 디렉토리 설정
WORKDIR /app

# 시스템 의존성 설치
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Python 의존성 파일 복사
COPY requirements.txt .

# Python 패키지 설치
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 애플리케이션 코드 복사
COPY ./app /app/app

# 테스트 코드 복사 (테스트 실행용)
COPY ./tests /app/tests
COPY ./pyproject.toml /app/pyproject.toml
COPY ./Makefile /app/Makefile

# 포트 노출 (문서화 목적)
EXPOSE ${PORT}

# 애플리케이션 실행 (환경변수 PORT 사용)
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT}"]
