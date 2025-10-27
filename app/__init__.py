# app/__init__.py
import os
from flask import Flask

# app 패키지 폴더 기준 절대경로
_THIS_DIR = os.path.abspath(os.path.dirname(__file__))
_TEMPLATE_DIR = os.path.join(_THIS_DIR, "templates")
_STATIC_DIR   = os.path.join(_THIS_DIR, "static")

def create_app():
    # ✅ Flask가 확실히 app/templates, app/static을 보게 절대경로로 지정
    app = Flask(__name__, template_folder=_TEMPLATE_DIR, static_folder=_STATIC_DIR)

    app.config.update(
        SECRET_KEY="change-this-in-prod",
        MAX_CONTENT_LENGTH=2 * 1024 * 1024 * 1024,  # ✅ 2GB로 증가 (기존 512MB)
    )

    # 업/출력 경로
    root = os.path.abspath(os.path.join(_THIS_DIR, ".."))
    uploads_root = os.path.join(root, "uploads")
    outputs_root = os.path.join(root, "outputs")
    app.config["UPLOAD_PDF_DIR"]  = os.path.join(uploads_root, "pdfs")
    app.config["UPLOAD_XLSX_DIR"] = os.path.join(uploads_root, "excels")
    app.config["OUTPUT_DIR"]      = outputs_root
    for p in [uploads_root, outputs_root, app.config["UPLOAD_PDF_DIR"], app.config["UPLOAD_XLSX_DIR"]]:
        os.makedirs(p, exist_ok=True)

    from .routes import bp as main_bp
    app.register_blueprint(main_bp)

    # 진단 로그 (콘솔에 실제 경로 찍힘)
    print("[Flask] template_folder =", app.template_folder)
    print("[Flask] static_folder   =", app.static_folder)
    print(f"[Flask] MAX_CONTENT_LENGTH = {app.config['MAX_CONTENT_LENGTH'] / (1024**3):.1f}GB")  # ✅ 용량 로그

    return app