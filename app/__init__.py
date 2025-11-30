# app/__init__.py
import os
from flask import Flask


def create_app():
    """Flask 애플리케이션 팩토리"""
    app = Flask(__name__)

    # 기본 설정
    app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
    app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024 * 1024  # 2GB

    # 업로드/출력 디렉토리 설정
    base_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

    app.config['UPLOAD_XLSX_DIR'] = os.path.join(base_dir, 'uploads', 'excels')
    app.config['UPLOAD_PDF_DIR'] = os.path.join(base_dir, 'uploads', 'pdfs')
    app.config['OUTPUT_DIR'] = os.path.join(base_dir, 'outputs')

    # 디렉토리 생성
    os.makedirs(app.config['UPLOAD_XLSX_DIR'], exist_ok=True)
    os.makedirs(app.config['UPLOAD_PDF_DIR'], exist_ok=True)
    os.makedirs(app.config['OUTPUT_DIR'], exist_ok=True)

    # 라우트 등록
    from app.routes import bp
    app.register_blueprint(bp)

    # 로깅 설정
    if not app.debug:
        import logging
        from logging.handlers import RotatingFileHandler

        log_dir = os.path.join(base_dir, 'logs')
        os.makedirs(log_dir, exist_ok=True)

        file_handler = RotatingFileHandler(
            os.path.join(log_dir, 'app.log'),
            maxBytes=102400000,  # 10MB
            backupCount=10
        )
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
        ))
        file_handler.setLevel(logging.INFO)
        app.logger.addHandler(file_handler)
        app.logger.setLevel(logging.INFO)
        app.logger.info('Application startup')

    return app