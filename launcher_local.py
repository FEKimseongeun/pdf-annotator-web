#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import webbrowser
import socket
import multiprocessing

from app import create_app

def _resource_base():
    """
    PyInstaller로 빌드되면 템플릿/정적파일은 임시폴더(_MEIPASS)로 풀림.
    그렇지 않으면 프로젝트 루트.
    """
    if hasattr(sys, "_MEIPASS"):
        return sys._MEIPASS
    return os.path.abspath(os.path.dirname(__file__))

def _pick_free_port(default=5000):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", default))
            return default
        except OSError:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

def main():
    # Windows + PyInstaller에서 멀티프로세싱(Executor) 안전하게
    multiprocessing.freeze_support()
    try:
        multiprocessing.set_start_method("spawn")
    except RuntimeError:
        pass

    base = _resource_base()

    # 템플릿/정적 파일 경로를 exe에서도 찾을 수 있도록 명시
    template_folder = os.path.join(base, "app", "templates")
    static_folder   = os.path.join(base, "app", "static")

    app = create_app()

    # 업로드/출력 경로가 실행 파일 폴더 기준으로 잡히도록 보정 (선택)
    # create_app()에서 이미 상대 경로를 만들었다면 생략 가능
    # 필요 시:
    # app.config["UPLOAD_PDF_DIR"]  = os.path.join(base, "uploads", "pdfs")
    # app.config["UPLOAD_XLSX_DIR"] = os.path.join(base, "uploads", "excels")
    # app.config["OUTPUT_DIR"]      = os.path.join(base, "outputs")
    for p in [app.config["UPLOAD_PDF_DIR"], app.config["UPLOAD_XLSX_DIR"], app.config["OUTPUT_DIR"]]:
        os.makedirs(p, exist_ok=True)

    port = _pick_free_port(5000)
    url  = f"http://127.0.0.1:{port}/"
    print(f" * Running on {url}")
    # 브라우저 자동 오픈
    webbrowser.open(url)

    # 개발 서버로도 충분 (로컬 전용)
    # 멀티프로세싱 사용하는 서비스 로직(ProcessPoolExecutor)은 별도 프로세스에서 동작하므로 ok
    app.run(host="127.0.0.1", port=port, debug=False, threaded=True)

if __name__ == "__main__":
    main()

