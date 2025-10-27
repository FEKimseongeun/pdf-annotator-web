#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import webbrowser
import socket
import multiprocessing
from pathlib import Path


def _pick_free_port(default=5000):
    """사용 가능한 포트 찾기"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", default))
            return default
        except OSError:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]


def setup_environment():
    """환경 설정"""
    # PyInstaller 동결 환경 체크
    is_frozen = getattr(sys, "frozen", False)

    if is_frozen:
        # exe 환경: ThreadPoolExecutor 사용
        os.environ["E2M_POOL"] = "thread"
        # ✅ 실행 파일 경로를 기준으로 작업 디렉토리 설정
        base_path = Path(sys.executable).parent
    else:
        # 개발 환경
        base_path = Path(__file__).parent

    # 환경변수 설정
    os.environ.setdefault('FLASK_ENV', 'production')

    return is_frozen, base_path


def main():
    """메인 실행 함수"""
    # multiprocessing 안전 설정
    multiprocessing.freeze_support()

    # 환경 설정
    is_frozen, base_path = setup_environment()

    # Flask 앱 생성
    try:
        from app import create_app
        app = create_app()  # ✅ 인자 제거 (create_app은 인자를 받지 않음)
    except Exception as e:
        print(f"❌ 앱 생성 실패: {e}")
        if not is_frozen:
            import traceback
            traceback.print_exc()
        input("Enter 키를 눌러 종료...")
        sys.exit(1)

    # ✅ 업로드/출력 폴더가 실행 파일 위치에 생성되도록 보장
    if is_frozen:
        for folder in ['uploads/pdfs', 'uploads/excels', 'outputs']:
            Path(base_path / folder).mkdir(parents=True, exist_ok=True)

    # 포트 선택 및 URL 생성
    port = _pick_free_port(5000)
    url = f"http://127.0.0.1:{port}/"

    # 시작 메시지
    print("=" * 60)
    print("  _____ _____ ___________ _     ___  __  __   _   _____  _  ___   _ ___ ")
    print(" | ____|_   _|_   _|  ___| |   |__ \\|  \\/  | /_\\ |  _  || |/ / | | | _ \\")
    print(" | |__   | |   | | | |__ | |     ) | |\\/| |/ _ \\| |_| ||    | |_| |  _/")
    print(" |  __   | |   | | |  __|| |    / /| |  | / ___ \\   _ || |\\ \\  _  | |  ")
    print(" | |___| |_|  _| |_| |___| |___|/_/ |_|  |_/   \\_\\_| |_||_| \\_\\| |_|_|  ")
    print(" |_____|___| |_____|_____|_____|                                         ")
    print("=" * 60)
    print(f"🚀 서버 시작: {url}")
    print(f"📁 작업 디렉토리: {Path.cwd()}")
    print(f"🔧 실행 모드: {'EXE (동결)' if is_frozen else '개발'}")
    print(f"⚙️  병렬 처리: {os.environ.get('E2M_POOL', 'process')}")
    print(f"💾 업로드 제한: {app.config.get('MAX_CONTENT_LENGTH', 0) / (1024**3):.1f}GB")
    print("=" * 60)
    print("💡 종료하려면 브라우저에서 '프로그램 종료' 버튼을 클릭하세요.")
    print("=" * 60)

    # 브라우저 자동 실행
    try:
        webbrowser.open(url)
    except Exception as e:
        print(f"⚠️  브라우저 자동 실행 실패: {e}")
        print(f"   수동으로 열어주세요: {url}")

    # 서버 실행
    try:
        app.run(
            host="127.0.0.1",
            port=port,
            debug=False,  # 프로덕션에서는 항상 False
            threaded=True,
            use_reloader=False  # exe에서 reloader 비활성화
        )
    except KeyboardInterrupt:
        print("\n🛑 사용자가 서버를 중지했습니다.")
    except Exception as e:
        print(f"\n❌ 서버 실행 오류: {e}")
        if not is_frozen:
            import traceback
            traceback.print_exc()
        input("Enter 키를 눌러 종료...")
        sys.exit(1)


if __name__ == "__main__":
    main()