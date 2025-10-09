import os
import google.generativeai as genai
from dotenv import load_dotenv

# .env 파일에서 환경 변수를 로드합니다.
load_dotenv()

# 환경 변수에서 API 키를 가져옵니다.
# .env 파일에 'GEMINI_API_KEY="AIzaSy..."' 형식으로 키가 있는지 확인하세요.
api_key = os.getenv("GEMINI_API_KEY")

# ⭐️ 이 코드가 가장 중요합니다!
# API 키가 없거나 로드되지 않았는지 확인합니다.
if not api_key:
    print("오류: GEMINI_API_KEY 환경 변수를 찾을 수 없습니다.")
    print(".env 파일에 올바르게 설정되었는지 확인하세요.")
else:
    try:
        # **해결의 핵심: API 키를 사용하도록 명시적으로 설정합니다.**
        genai.configure(api_key=api_key)

        # 유효한 모델을 생성합니다.
        model = genai.GenerativeModel(model_name="gemini-2.5-flash-lite")

        # 콘텐츠 생성을 요청합니다.
        response = model.generate_content("AI가 어떻게 작동하는지 몇 단어로 설명해 줘")
        print(response.text)

    except Exception as e:
        # 발생할 수 있는 모든 오류를 출력합니다.
        print(f"오류가 발생했습니다: {e}")