import os
from flask import Flask

def create_app():
    app = Flask(__name__, instance_relative_config=False)
    app.config.update(
        SECRET_KEY="change-this-in-prod",
        MAX_CONTENT_LENGTH=512 * 1024 * 1024,  # 512MB
        BASE_DIR=os.path.abspath(os.path.dirname(os.path.dirname(__file__))),
        TEMPLATES_AUTO_RELOAD=True,
        SEND_FILE_MAX_AGE_DEFAULT=0,
    )
    app.jinja_env.auto_reload = True

    # 프로젝트 루트 기준 경로
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    uploads_root = os.path.join(root, "uploads")
    outputs_root = os.path.join(root, "outputs")

    # 하위 디렉토리
    app.config["UPLOAD_PDF_DIR"]   = os.path.join(uploads_root, "pdfs")
    app.config["UPLOAD_XLSX_DIR"]  = os.path.join(uploads_root, "excels")
    app.config["OUTPUT_DIR"]       = outputs_root

    for path in [uploads_root, outputs_root, app.config["UPLOAD_PDF_DIR"], app.config["UPLOAD_XLSX_DIR"]]:
        os.makedirs(path, exist_ok=True)

    from .routes import bp as main_bp
    app.register_blueprint(main_bp)

    return app
