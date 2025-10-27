import os
import secrets
from pathlib import Path

# 프로젝트 루트 디렉토리
BASE_DIR = Path(__file__).parent.absolute()


class Config:
    """기본 설정"""
    # 보안 키 (프로덕션에서는 환경변수로 관리)
    SECRET_KEY = os.environ.get('SECRET_KEY') or secrets.token_hex(32)

    # 업로드 설정
    MAX_CONTENT_LENGTH = 512 * 1024 * 1024  # 512MB
    ALLOWED_EXTENSIONS_PDF = {'pdf'}
    ALLOWED_EXTENSIONS_EXCEL = {'xlsx', 'xls'}

    # 디렉토리 설정
    UPLOAD_PDF_DIR = BASE_DIR / "uploads" / "pdfs"
    UPLOAD_XLSX_DIR = BASE_DIR / "uploads" / "excels"
    OUTPUT_DIR = BASE_DIR / "outputs"

    # 로그 설정
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    LOG_FILE = BASE_DIR / "app.log"

    # 병렬 처리 설정
    MAX_WORKERS = max(1, (os.cpu_count() or 4) - 1)

    @classmethod
    def init_app(cls, app):
        """앱 초기화 시 필요한 디렉토리 생성"""
        for directory in [cls.UPLOAD_PDF_DIR, cls.UPLOAD_XLSX_DIR, cls.OUTPUT_DIR]:
            directory.mkdir(parents=True, exist_ok=True)


class DevelopmentConfig(Config):
    """개발 환경 설정"""
    DEBUG = True
    TESTING = False


class ProductionConfig(Config):
    """프로덕션 환경 설정"""
    DEBUG = False
    TESTING = False


class TestingConfig(Config):
    """테스트 환경 설정"""
    DEBUG = True
    TESTING = True


config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}