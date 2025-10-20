#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import webbrowser
import socket
import multiprocessing

from app import create_app


def _pick_free_port(default=5000):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", default))
            return default
        except OSError:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]


def main():
    # ✅ PyInstaller 환경에서만 ThreadPoolExecutor 사용
    is_frozen = getattr(sys, "frozen", False)
    if is_frozen:
        os.environ["E2M_POOL"] = "thread"

    # ✅ multiprocessing 안전 설정
    multiprocessing.freeze_support()

    # Flask 앱 생성 (app/__init__.py의 경로 설정 사용)
    app = create_app()

    # ✅ 업로드/출력 폴더 생성
    for p in [app.config["UPLOAD_PDF_DIR"], app.config["UPLOAD_XLSX_DIR"], app.config["OUTPUT_DIR"]]:
        os.makedirs(p, exist_ok=True)

    port = _pick_free_port(5000)
    url = f"http://127.0.0.1:{port}/"
    print(f" * Running on {url}")

    webbrowser.open(url)

    app.run(host="127.0.0.1", port=port, debug=False, threaded=True)


if __name__ == "__main__":
    main()